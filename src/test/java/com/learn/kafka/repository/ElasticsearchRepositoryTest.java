package com.learn.kafka.repository;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.ElasticsearchIndicesClient;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import co.elastic.clients.elasticsearch.core.bulk.OperationType;
import com.learn.kafka.model.ExchangeRate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ElasticsearchRepositoryTest {

    @Mock
    private ElasticsearchClient elasticsearchClient;

    @Mock
    private ElasticsearchIndicesClient indicesClient;

    @InjectMocks
    private ElasticsearchRepository elasticsearchRepository;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        when(elasticsearchClient.indices()).thenReturn(indicesClient);
        ReflectionTestUtils.setField(elasticsearchRepository, "indexName", "test-exchange-rates");
    }

    /**
     * Teste la méthode `indexAll` pour s'assurer qu'elle vérifie l'existence de l'index,
     * crée l'index si nécessaire, et indexe les données en utilisant une requête Bulk.
     */
    @Test
    void testIndexAll() throws IOException {
        // Simuler la réponse pour vérifier l'existence de l'index
        when(indicesClient.exists(any(ExistsRequest.class))).thenReturn(new BooleanResponse(false));

        // Simuler la création de l'index avec une réponse positive
        CreateIndexResponse mockCreateIndexResponse = new CreateIndexResponse.Builder()
                .acknowledged(true)
                .shardsAcknowledged(true)
                .index("test-exchange-rates")
                .build();
        when(indicesClient.create(any(CreateIndexRequest.class))).thenReturn(mockCreateIndexResponse);

        // Simuler une réponse Bulk avec un élément réussi
        BulkResponseItem mockBulkResponseItem = new BulkResponseItem.Builder()
                .id("1")
                .index("test-exchange-rates")
                .result("created")
                .operationType(OperationType.Index)
                .status(201)
                .build();
        BulkResponse mockBulkResponse = new BulkResponse.Builder()
                .errors(false)
                .items(Collections.singletonList(mockBulkResponseItem))
                .took(1)
                .build();
        ArgumentCaptor<BulkRequest> bulkRequestCaptor = ArgumentCaptor.forClass(BulkRequest.class);
        when(elasticsearchClient.bulk(bulkRequestCaptor.capture())).thenReturn(mockBulkResponse);

        // Préparer les données à indexer
        ExchangeRate rate = new ExchangeRate();
        rate.setBase("USD");
        rate.setCurrency("EUR");
        rate.setRate(0.88);

        // Appeler la méthode à tester
        elasticsearchRepository.indexAll(List.of(rate));

        // Vérifier que la méthode `exists` a été appelée
        verify(indicesClient, times(1)).exists(any(ExistsRequest.class));

        // Vérifier que la méthode `create` a été appelée pour créer l'index
        verify(indicesClient, times(1)).create(any(CreateIndexRequest.class));

        // Vérifier que la méthode `bulk` a été appelée pour indexer les données
        verify(elasticsearchClient, times(1)).bulk(any(BulkRequest.class));

        // Capturer et vérifier le contenu de la requête Bulk
        BulkRequest capturedBulkRequest = bulkRequestCaptor.getValue();
        assertNotNull(capturedBulkRequest);
        assertEquals(1, capturedBulkRequest.operations().size());

        BulkOperation bulkOperation = capturedBulkRequest.operations().get(0);
        assertTrue(bulkOperation.isIndex());
        IndexOperation<?> indexOperation = bulkOperation.index();
        assertEquals("test-exchange-rates", indexOperation.index());
        assertEquals(rate, indexOperation.document());
    }
}
