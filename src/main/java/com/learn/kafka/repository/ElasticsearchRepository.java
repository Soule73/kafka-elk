package com.learn.kafka.repository;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import com.learn.kafka.model.ExchangeRate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

@Repository
public class ElasticsearchRepository {

    @Autowired
    private ElasticsearchClient elasticsearchClient;

    @Value("${elasticsearch.index.name}")
    private String indexName;

    public void indexAll(List<ExchangeRate> rates) {
        try {
            // Vérifier si l'index existe, sinon le créer avec un mapping explicite
            if (!elasticsearchClient.indices().exists(e -> e.index(indexName)).value()) {
                createIndexWithMapping();
            }

            // Préparer les opérations Bulk
            List<BulkOperation> operations = rates.stream()
                    .map(rate -> BulkOperation.of(op -> op
                            .index(idx -> idx
                                    .index(indexName)
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

    private void createIndexWithMapping() throws IOException {
        CreateIndexRequest request = new CreateIndexRequest.Builder()
                .index(indexName)
                .mappings(m -> m
                        .properties("timestamp", p -> p.date(d -> d))
                        .properties("base", p -> p.keyword(k -> k))
                        .properties("currency", p -> p.keyword(k -> k))
                        .properties("rate", p -> p.double_(d -> d))
                )
                .build();

        CreateIndexResponse response = elasticsearchClient.indices().create(request);

        if (!response.acknowledged()) {
            throw new RuntimeException("La création de l'index Elasticsearch a échoué.");
        }
    }
}
