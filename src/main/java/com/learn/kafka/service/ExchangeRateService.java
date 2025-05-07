package com.learn.kafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.kafka.model.ExchangeRate;
import com.learn.kafka.repository.ElasticsearchRepository;
import com.learn.kafka.repository.MongoDBRepository;
<<<<<<< HEAD
import com.learn.kafka.repository.KafkaRepository;
import org.springframework.beans.factory.annotation.Value;
=======
import org.springframework.kafka.core.KafkaTemplate;
>>>>>>> c6756da (Upgrade packages to latest versions)
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

<<<<<<< HEAD
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
=======
import java.io.IOException;
import java.time.Instant;
>>>>>>> c6756da (Upgrade packages to latest versions)
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class ExchangeRateService {

    private static final Logger logger = LoggerFactory.getLogger(ExchangeRateService.class);

    private final ElasticsearchRepository elasticsearchRepository;
    private final MongoDBRepository mongoDBRepository;
<<<<<<< HEAD
    private final KafkaRepository kafkaRepository;
    private final RestTemplate restTemplate = new RestTemplate();

    @Value("${exchange-rate.api.url}")
    private String apiUrl;

=======
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final RestTemplate restTemplate = new RestTemplate(); // Utilisation directe de RestTemplate

    private final String apiUrl = "https://api.exchangerate-api.com/v4/latest/USD";
>>>>>>> c6756da (Upgrade packages to latest versions)
    private final ObjectMapper objectMapper = new ObjectMapper();

    public ExchangeRateService(ElasticsearchRepository elasticsearchRepository,
                               MongoDBRepository mongoDBRepository,
<<<<<<< HEAD
                               KafkaRepository kafkaRepository) {
        this.elasticsearchRepository = elasticsearchRepository;
        this.mongoDBRepository = mongoDBRepository;
        this.kafkaRepository = kafkaRepository;
=======
                               KafkaTemplate<String, String> kafkaTemplate) {
        this.elasticsearchRepository = elasticsearchRepository;
        this.mongoDBRepository = mongoDBRepository;
        this.kafkaTemplate = kafkaTemplate;
>>>>>>> c6756da (Upgrade packages to latest versions)
    }

    public List<ExchangeRate> formatExchangeRates(Map<String, Object> response) {
        logger.info("Formattage des taux de change...");
        List<ExchangeRate> formattedRates = new ArrayList<>();
<<<<<<< HEAD

        // Extraire les informations de base
        String baseCurrency = (String) response.get("base_code");

        // Utiliser l'heure actuelle comme timestamp
        String timestamp = ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
=======
        String baseCurrency = (String) response.get("base");
        String date = (String) response.get("date");
>>>>>>> c6756da (Upgrade packages to latest versions)

        @SuppressWarnings("unchecked")
        Map<String, Number> rates = (Map<String, Number>) response.get("rates");

        rates.forEach((currency, rate) -> {
            ExchangeRate exchangeRate = new ExchangeRate();
            exchangeRate.setBase(baseCurrency);
            exchangeRate.setCurrency(currency);
            exchangeRate.setRate(rate.doubleValue());
<<<<<<< HEAD
            exchangeRate.setTimestamp(timestamp);
            exchangeRate.setDate(ZonedDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)); 
=======
            exchangeRate.setTimestamp(Instant.now().toString());
            exchangeRate.setDate(date);
>>>>>>> c6756da (Upgrade packages to latest versions)
            formattedRates.add(exchangeRate);
        });

        logger.info("Formattage terminé. Nombre de devises formatées : {}", formattedRates.size());
        return formattedRates;
    }

<<<<<<< HEAD
    public String fetchAndStoreExchangeRates() {
        logger.info("Début de la récupération des taux de change depuis l'API...");
        @SuppressWarnings("unchecked")
        Map<String, Object> response = restTemplate.getForObject(apiUrl, Map.class);

        if (response == null || !"success".equals(response.get("result"))) {
=======
    public String fetchAndStoreExchangeRates() throws IOException {
        logger.info("Début de la récupération des taux de change depuis l'API...");
        @SuppressWarnings("unchecked")
        Map<String, Object> response = restTemplate.getForObject(apiUrl, Map.class); // Utilisation directe de RestTemplate

        if (response == null || !response.containsKey("rates")) {
>>>>>>> c6756da (Upgrade packages to latest versions)
            throw new RuntimeException("Impossible de récupérer les données de l'API.");
        }

        logger.info("Données récupérées avec succès depuis l'API.");

<<<<<<< HEAD
        List<ExchangeRate> formattedRates = formatExchangeRates(response);

=======
        // Formater les données
        List<ExchangeRate> formattedRates = formatExchangeRates(response);

        // Publier sur Kafka
>>>>>>> c6756da (Upgrade packages to latest versions)
        logger.info("Publication des taux sur Kafka...");
        for (ExchangeRate rate : formattedRates) {
            try {
                String rateJson = objectMapper.writeValueAsString(rate);
<<<<<<< HEAD
                kafkaRepository.sendMessageToDefaultTopic(rateJson);
=======
                kafkaTemplate.send("exchange-rates-topic", rateJson);
>>>>>>> c6756da (Upgrade packages to latest versions)
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
