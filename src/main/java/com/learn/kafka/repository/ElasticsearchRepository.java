package com.learn.kafka.repository;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.transport.endpoints.BooleanResponse;
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

/**
 * Repository pour interagir avec Elasticsearch.
 * Fournit des méthodes pour indexer des données et gérer les index.
 */
@Repository
public class ElasticsearchRepository {

    /**
     * Client Elasticsearch pour interagir avec le cluster Elasticsearch.
     * Utilisé pour exécuter des requêtes et indexer des données.
     */
    @Autowired
    private ElasticsearchClient elasticsearchClient;

    /**
     * Nom de l'index Elasticsearch.
     * Peut être configuré via les propriétés de l'application.
     */
    @Value("${elasticsearch.index.name:exchange-rates}")
    private String indexName;

    /**
     * Indexe une liste d'objets ExchangeRate dans Elasticsearch.
     * 
     * @param rates la liste des taux de change à indexer.
     */
    public void indexAll(List<ExchangeRate> rates) {
        try {
            if (!indexExists()) {
                createIndexWithMapping();
            }

            List<BulkOperation> operations = prepareBulkOperations(rates);
            executeBulkRequest(operations);
        } catch (IOException e) {
            throw new RuntimeException("Erreur lors de l'indexation en Bulk dans Elasticsearch : " + e.getMessage());
        }
    }

    /**
     * Vérifie si l'index Elasticsearch existe.
     * 
     * @return true si l'index existe, false sinon.
     * @throws IOException si une erreur survient lors de la vérification.
     */
    private boolean indexExists() throws IOException {
        ExistsRequest existsRequest = new ExistsRequest.Builder()
                .index(indexName)
                .build();
        BooleanResponse existsResponse = elasticsearchClient.indices().exists(existsRequest);
        return existsResponse != null && existsResponse.value();
    }

    /**
     * Prépare une liste d'opérations Bulk pour indexer les données.
     * 
     * @param rates la liste des taux de change à indexer.
     * @return une liste d'opérations Bulk.
     */
    private List<BulkOperation> prepareBulkOperations(List<ExchangeRate> rates) {
        return rates.stream()
                .map(rate -> BulkOperation.of(op -> op
                        .index(idx -> idx
                                .index(indexName)
                                .document(rate)
                        )
                ))
                .collect(Collectors.toList());
    }

    /**
     * Exécute une requête Bulk pour indexer les données dans Elasticsearch.
     * 
     * @param operations la liste des opérations Bulk à exécuter.
     * @throws IOException si une erreur survient lors de l'exécution de la requête.
     */
    private void executeBulkRequest(List<BulkOperation> operations) throws IOException {
        BulkRequest bulkRequest = new BulkRequest.Builder()
                .operations(operations)
                .build();

        BulkResponse response = elasticsearchClient.bulk(bulkRequest);

        if (response.errors()) {
            throw new RuntimeException("Certaines opérations Bulk ont échoué : " + response.items());
        }
    }

    /**
     * Crée un index dans Elasticsearch avec un mapping explicite.
     * 
     * @throws IOException si une erreur survient lors de la création de l'index.
     * @return void
     */
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
