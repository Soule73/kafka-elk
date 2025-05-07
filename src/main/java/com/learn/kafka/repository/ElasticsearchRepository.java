package com.learn.kafka.repository;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import com.learn.kafka.model.ExchangeRate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.List;

@Repository
public class ElasticsearchRepository {

    @Autowired
    private ElasticsearchClient elasticsearchClient;

    public void indexAll(List<ExchangeRate> rates) {
        rates.forEach(rate -> {
            try {
                IndexRequest<ExchangeRate> request = new IndexRequest.Builder<ExchangeRate>()
                        .index("exchange-rates")
                        .document(rate)
                        .build();
                elasticsearchClient.index(request);
            } catch (IOException e) {
                throw new RuntimeException("Erreur lors de l'indexation dans Elasticsearch : " + e.getMessage());
            }
        });
    }
}
