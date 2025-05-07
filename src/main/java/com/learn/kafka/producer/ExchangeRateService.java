package com.learn.kafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.kafka.model.ExchangeRate;
import com.learn.kafka.repository.ExchangeRateRepository;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class ExchangeRateService {

    private static final Logger logger = LoggerFactory.getLogger(ExchangeRateService.class);

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ExchangeRateRepository exchangeRateRepository;

    private final String apiUrl = "https://api.exchangerate-api.com/v4/latest/USD";

    private final ObjectMapper objectMapper = new ObjectMapper(); // Pour convertir en JSON

    public List<ExchangeRate> formatExchangeRates(Map<String, Object> response) {
        logger.info("Formattage des taux de change...");
        List<ExchangeRate> formattedRates = new ArrayList<>();
        String baseCurrency = (String) response.get("base");
        String date = (String) response.get("date");
        Map<String, Number> rates = (Map<String, Number>) response.get("rates");

        rates.forEach((currency, rate) -> {
            ExchangeRate exchangeRate = new ExchangeRate();
            exchangeRate.setBase(baseCurrency);
            exchangeRate.setCurrency(currency);
            exchangeRate.setRate(rate.doubleValue());
            exchangeRate.setTimestamp(Instant.now().toString());
            exchangeRate.setDate(date);
            formattedRates.add(exchangeRate);
        });

        logger.info("Formattage terminé. Nombre de devises formatées : {}", formattedRates.size());
        return formattedRates;
    }

    public void deleteOldData() {
        logger.info("Suppression des anciennes données dans Elasticsearch...");
        try {
            DeleteByQueryRequest request = new DeleteByQueryRequest("exchange-rates");
            request.setQuery(QueryBuilders.rangeQuery("timestamp").lt("now-7d/d")); // Supprimer les données de plus de 7 jours
            request.setConflicts("proceed"); // Ignorer les conflits
            restHighLevelClient.deleteByQuery(request, RequestOptions.DEFAULT);
            logger.info("Suppression des anciennes données terminée.");
        } catch (Exception e) {
            logger.error("Erreur lors de la suppression des anciennes données : {}", e.getMessage());
        }
    }

    public String fetchAndStoreExchangeRates() throws IOException {
        logger.info("Début de la récupération des taux de change depuis l'API...");
        RestTemplate restTemplate = new RestTemplate();
        Map<String, Object> response = restTemplate.getForObject(apiUrl, Map.class);

        if (response == null || !response.containsKey("rates")) {
            logger.error("Erreur : Impossible de récupérer les données de l'API.");
            throw new RuntimeException("Impossible de récupérer les données de l'API.");
        }

        logger.info("Données récupérées avec succès depuis l'API.");

        // Supprimer les anciennes données avant d'indexer les nouvelles
        deleteOldData();

        // Formater les données
        List<ExchangeRate> formattedRates = formatExchangeRates(response);

        // Publier chaque taux sur Kafka
        logger.info("Publication des taux sur Kafka...");
        for (ExchangeRate rate : formattedRates) {
            try {
                String rateJson = objectMapper.writeValueAsString(rate); // Convertir en JSON
                kafkaTemplate.send("exchange-rates-topic", rateJson);
            } catch (JsonProcessingException e) {
                logger.error("Erreur lors de la conversion en JSON : {}", e.getMessage());
            }
        }
        logger.info("Publication sur Kafka terminée.");

        // Insérer les données dans MongoDB
        logger.info("Insertion des taux dans MongoDB...");
        try {
            exchangeRateRepository.insert(formattedRates);
            logger.info("Insertion dans MongoDB terminée. Nombre de documents insérés : {}", formattedRates.size());
        } catch (Exception e) {
            logger.error("Erreur lors de l'insertion dans MongoDB : {}", e.getMessage());
        }

        // Indexer chaque taux de change comme un document distinct dans Elasticsearch
        logger.info("Indexation des taux dans Elasticsearch...");
        for (ExchangeRate rate : formattedRates) {
            try {
                String rateJson = objectMapper.writeValueAsString(rate); // Convertir en JSON
                IndexRequest request = new IndexRequest("exchange-rates")
                        .source(rateJson, XContentType.JSON);
                restHighLevelClient.index(request, RequestOptions.DEFAULT);
            } catch (JsonProcessingException e) {
                logger.error("Erreur lors de la conversion en JSON pour Elasticsearch : {}", e.getMessage());
            }
        }
        logger.info("Indexation dans Elasticsearch terminée.");

        return "Données publiées, stockées dans MongoDB et indexées avec succès.";
    }
}

