package com.learn.kafka.service;

import com.learn.kafka.model.ExchangeRate;
import com.learn.kafka.repository.ElasticsearchRepository;
import com.learn.kafka.repository.KafkaRepository;
import com.learn.kafka.repository.MongoDBRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ExchangeRateServiceTest {
    /**
     * URL de l'API pour récupérer les taux de change.
     */
    private String apiUrl = "https://open.er-api.com/v6/latest/USD";


    /*
     * Repository pour interagir avec Elasticsearch.
     * Fournit des méthodes pour indexer des données dans Elasticsearch.
     */
    @Mock
    private ElasticsearchRepository elasticsearchRepository;

    /*
     * Repository pour interagir avec MongoDB.
     * Fournit des méthodes pour insérer des données dans MongoDB.
     */
    @Mock
    private MongoDBRepository mongoDBRepository;

    /**
     * Repository pour interagir avec Kafka.
     * Fournit des méthodes pour publier des messages sur Kafka.
     */
    @Mock
    private KafkaRepository kafkaRepository;

    /**
     * Client RestTemplate pour effectuer des appels HTTP.
     */
    @Mock
    private RestTemplate restTemplate;

    /*
     * ExchangeRateService est le service principal qui gère les taux de change.
     * Il utilise les repositories pour interagir avec Elasticsearch, MongoDB et Kafka.
     */
    @InjectMocks
    @Autowired
    private ExchangeRateService exchangeRateService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        exchangeRateService.setApiUrl(apiUrl);
    }

    /**
     * Teste la méthode `formatExchangeRates` pour s'assurer qu'elle formate correctement
     * les données de réponse de l'API en une liste d'objets `ExchangeRate`.
     */
    @Test
    void testFormatExchangeRates() {
        Map<String, Object> mockResponse = Map.of(
                "base_code", "USD",
                "rates", Map.of("EUR", 0.88, "GBP", 0.75)
        );

        List<ExchangeRate> rates = exchangeRateService.formatExchangeRates(mockResponse);

        assertEquals(2, rates.size());
        assertEquals("USD", rates.get(0).getBase());
        assertTrue(rates.stream().anyMatch(rate -> "EUR".equals(rate.getCurrency())));
        assertTrue(rates.stream().anyMatch(rate -> "GBP".equals(rate.getCurrency())));
    }

    /**
     * Teste la méthode `fetchAndStoreExchangeRates` pour s'assurer qu'elle récupère les données
     * de l'API, les formate, les publie sur Kafka, les stocke dans MongoDB et les indexe dans Elasticsearch.
     */
    @Test
    void testFetchAndStoreExchangeRates() {
        Map<String, Object> mockResponse = Map.of(
                "result", "success",
                "base_code", "USD",
                "rates", Map.of("EUR", 0.88, "GBP", 0.75, "JPY", 143.53)
        );

        when(restTemplate.getForObject(eq(apiUrl), eq(Map.class))).thenReturn(mockResponse);

        String result = exchangeRateService.fetchAndStoreExchangeRates();

        assertEquals("Données publiées, stockées dans MongoDB et indexées avec succès.", result);

        verify(kafkaRepository, times(3)).sendMessageToDefaultTopic(anyString());
        verify(mongoDBRepository, times(1)).saveAll(anyList());
        verify(elasticsearchRepository, times(1)).indexAll(anyList());
        verify(restTemplate, times(1)).getForObject(eq(apiUrl), eq(Map.class));
    }
}
