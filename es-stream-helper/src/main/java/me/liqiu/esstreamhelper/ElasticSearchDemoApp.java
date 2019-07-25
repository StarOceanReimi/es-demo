package me.liqiu.esstreamhelper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.query.*;
import org.springframework.web.bind.annotation.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;

@SpringBootApplication
@RestController
@Slf4j
public class ElasticSearchDemoApp {

    @Autowired
    ElasticsearchTemplate template;

    @Autowired
    ObjectMapper mapper;

    private static final String INDEX_NAME = "mydata";

    @GetMapping("/{type}/all")
    public List<Map>  findAll(@PathVariable("type") String type) {

        SearchQuery query = new NativeSearchQueryBuilder()
                .withQuery(new MatchAllQueryBuilder())
                .withTypes(type).build();

        return template.query(query, res -> stream(res.getHits().getHits())
                .map(hit -> {
                    val map = hit.getSourceAsMap();
                    map.put("_id", hit.getId());
                    return map;
                })
                .collect(toList()));
    }

    @GetMapping("/delete/{type}/{id}")
    public String deleteSomeDataToES(@PathVariable("type") String type, @PathVariable("id") String id) {

        log.info("deleting: {}/{} ...", type, id);

        DeleteQuery query = new DeleteQuery();
        query.setIndex(INDEX_NAME);
        query.setType(type);
        query.setQuery(new MatchQueryBuilder("_id", id));
        template.delete(query);

        return "ok";
    }


    @GetMapping("/insert/{type}")
    public String insertSomeDataToES(@PathVariable("type") String type, @RequestParam Map<String, Object> data) {

        IndexQuery query = null;
        try {
            val _source = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(data);
            log.info("source: {}\n", _source);
            query = new IndexQueryBuilder().withIndexName(INDEX_NAME)
                    .withType(type)
                    .withSource(_source)
                    .build();
            return template.index(query);
        } catch (JsonProcessingException e) {
            log.error("JsonProcessError: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @GetMapping("/update/{type}/{id}")
    public String updateSomeDataToES(@PathVariable("type") String type, @PathVariable("id") String id, @RequestParam Map<String, Object> data) {

        try {
            UpdateRequest request = new UpdateRequest(INDEX_NAME, type, id);
            val _source = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(data);
            log.info("source: {}\n", _source);
            request.doc(data, XContentType.JSON);
            request.upsert(data, XContentType.JSON);
            UpdateQuery query = new UpdateQueryBuilder()
                    .withIndexName(INDEX_NAME)
                    .withType(type)
                    .withId(id)
                    .withUpdateRequest(request)
                    .withDoUpsert(true).build();

            return template.update(query).toString();

        } catch (JsonProcessingException e) {
            log.error("JsonProcessError: {}", e.getMessage());
            throw new RuntimeException(e);
        }


    }


    public static void main(String[] args) {

        SpringApplication.run(ElasticSearchDemoApp.class, args);
    }

}
