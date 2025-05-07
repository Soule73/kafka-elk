package com.learn.kafka.producer;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class ElasticsearchService {

    @Autowired
    private ElasticsearchClient elasticsearchClient;

    public String indexDocument(String index, String id, Object document) throws IOException {
        IndexRequest<Object> request = new IndexRequest.Builder<>()
                .index(index)
                .id(id)
                .document(document)
                .build();

        IndexResponse response = elasticsearchClient.index(request);
        return response.id(); // Retourner l'ID du document indexé
    }
}
