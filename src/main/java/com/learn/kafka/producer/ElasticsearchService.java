package com.learn.kafka.producer;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Map;

@Service
public class ElasticsearchService {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    public String indexDocument(String index, String id, Map<String, Object> document) throws IOException {
        IndexRequest request = new IndexRequest(index)
                .id(id)
                .source(document, XContentType.JSON);

        IndexResponse response = restHighLevelClient.index(request, RequestOptions.DEFAULT);
        return response.getId();
    }
}
