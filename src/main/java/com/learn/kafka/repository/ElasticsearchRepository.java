package com.learn.kafka.repository;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import com.learn.kafka.model.ExchangeRate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

@Repository
public class ElasticsearchRepository {

    @Autowired
    private ElasticsearchClient elasticsearchClient;

    public void indexAll(List<ExchangeRate> rates) {
        try {
            // Préparer les opérations Bulk
            List<BulkOperation> operations = rates.stream()
                    .map(rate -> BulkOperation.of(op -> op
                            .index(idx -> idx
                                    .index("exchange-rates")
                                    .document(rate)
                            )
                    ))
                    .collect(Collectors.toList());

            // Construire et exécuter la requête Bulk
            BulkRequest bulkRequest = new BulkRequest.Builder()
                    .operations(operations)
                    .build();

            BulkResponse response = elasticsearchClient.bulk(bulkRequest);

            // Vérifier les erreurs dans la réponse
            if (response.errors()) {
                throw new RuntimeException("Certaines opérations Bulk ont échoué : " + response.items());
            }
        } catch (IOException e) {
            throw new RuntimeException("Erreur lors de l'indexation en Bulk dans Elasticsearch : " + e.getMessage());
        }
    }
}
