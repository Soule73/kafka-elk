package com.learn.kafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.kafka.model.ExchangeRate;
import com.learn.kafka.repository.ElasticsearchRepository;
import com.learn.kafka.repository.MongoDBRepository;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class ExchangeRateService {

    private static final Logger logger = LoggerFactory.getLogger(ExchangeRateService.class);

    private final ElasticsearchRepository elasticsearchRepository;
    private final MongoDBRepository mongoDBRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final RestTemplate restTemplate = new RestTemplate(); // Utilisation directe de RestTemplate

    private final String apiUrl = "https://api.exchangerate-api.com/v4/latest/USD";
    private final ObjectMapper objectMapper = new ObjectMapper();

    public ExchangeRateService(ElasticsearchRepository elasticsearchRepository,
                               MongoDBRepository mongoDBRepository,
                               KafkaTemplate<String, String> kafkaTemplate) {
        this.elasticsearchRepository = elasticsearchRepository;
        this.mongoDBRepository = mongoDBRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    public List<ExchangeRate> formatExchangeRates(Map<String, Object> response) {
        logger.info("Formattage des taux de change...");
        List<ExchangeRate> formattedRates = new ArrayList<>();
        String baseCurrency = (String) response.get("base");
        String date = (String) response.get("date");

        @SuppressWarnings("unchecked")
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

    public String fetchAndStoreExchangeRates() throws IOException {
        logger.info("Début de la récupération des taux de change depuis l'API...");
        @SuppressWarnings("unchecked")
        Map<String, Object> response = restTemplate.getForObject(apiUrl, Map.class); // Utilisation directe de RestTemplate

        if (response == null || !response.containsKey("rates")) {
            throw new RuntimeException("Impossible de récupérer les données de l'API.");
        }

        logger.info("Données récupérées avec succès depuis l'API.");

        // Formater les données
        List<ExchangeRate> formattedRates = formatExchangeRates(response);

        // Publier sur Kafka
        logger.info("Publication des taux sur Kafka...");
        for (ExchangeRate rate : formattedRates) {
            try {
                String rateJson = objectMapper.writeValueAsString(rate);
                kafkaTemplate.send("exchange-rates-topic", rateJson);
            } catch (JsonProcessingException e) {
                logger.error("Erreur lors de la conversion en JSON : {}", e.getMessage());
            }
        }
        logger.info("Publication sur Kafka terminée.");

        // Stocker dans MongoDB
        mongoDBRepository.saveAll(formattedRates);

        // Indexer dans Elasticsearch
        elasticsearchRepository.indexAll(formattedRates);

        return "Données publiées, stockées dans MongoDB et indexées avec succès.";
    }
}
