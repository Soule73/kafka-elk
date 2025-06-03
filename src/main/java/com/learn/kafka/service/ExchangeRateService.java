package com.learn.kafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.kafka.model.ExchangeRate;
import com.learn.kafka.repository.ElasticsearchRepository;
import com.learn.kafka.repository.MongoDBRepository;
import com.learn.kafka.repository.KafkaRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Service pour gérer les taux de change.
 * Fournit des méthodes pour récupérer, formater, publier et stocker les taux de change.
 */
@Service
public class ExchangeRateService {

    private static final Logger logger = LoggerFactory.getLogger(ExchangeRateService.class);

    /**
     * Repository pour interagir avec Elasticsearch.
     * Fournit des méthodes pour indexer des données dans Elasticsearch.
     */
    @Autowired
    private ElasticsearchRepository elasticsearchRepository;

    /**
     * Repository pour interagir avec MongoDB.
     * Fournit des méthodes pour insérer des données dans MongoDB.
     */
    @Autowired
    private MongoDBRepository mongoDBRepository;

    /**
     * Repository pour interagir avec Kafka.
     * Fournit des méthodes pour publier des messages sur Kafka.
     */
    @Autowired
    private KafkaRepository kafkaRepository;

    /**
     * Client RestTemplate pour effectuer des appels HTTP.
     */
    @Autowired
    private RestTemplate restTemplate;

    /**
     * URL de l'API pour récupérer les taux de change.
     */
    @Value("${exchange-rate.api.url}")
    private String apiUrl;

    /**
     * Mapper pour convertir les objets en JSON.
     */
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Récupère l'URL de l'API.
     * 
     * @return l'URL de l'API.
     * @see String
     */
    public void setApiUrl(String apiUrl) {
        this.apiUrl = apiUrl;
    }

    /**
     * Formate les données de réponse de l'API en une liste d'objets ExchangeRate.
     * 
     * @param response la réponse brute de l'API.
     * @return une liste d'objets ExchangeRate formatés.
     * @see ExchangeRate
     */
    public List<ExchangeRate> formatExchangeRates(Map<String, Object> response) {
        logger.info("Formattage des taux de change...");
        String baseCurrency = extractBaseCurrency(response);
        String timestamp = getCurrentTimestamp();
        Map<String, Number> rates = extractRates(response);

        List<ExchangeRate> formattedRates = createExchangeRates(baseCurrency, timestamp, rates);

        logger.info("Formattage terminé. Nombre de devises formatées : {}", formattedRates.size());
        return formattedRates;
    }

    /**
     * Extrait la devise de base de la réponse de l'API.
     * 
     * @param response la réponse brute de l'API.
     * @return la devise de base.
     * 
     */
    private String extractBaseCurrency(Map<String, Object> response) {
        return (String) response.get("base_code");
    }

    /**
     * Récupère le timestamp actuel au format ISO_OFFSET_DATE_TIME.
     * 
     * @return le timestamp actuel.
     * @see ZonedDateTime
     * @see DateTimeFormatter
     */
    private String getCurrentTimestamp() {
        return ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    }

    /**
     * Extrait les taux de change de la réponse de l'API.
     * 
     * @param response la réponse brute de l'API.
     * @return une map contenant les devises et leurs taux de change.
     * @see ExchangeRate
     */
    @SuppressWarnings("unchecked")
    private Map<String, Number> extractRates(Map<String, Object> response) {
        return (Map<String, Number>) response.get("rates");
    }

    /**
     * Crée une liste d'objets ExchangeRate à partir des données extraites.
     * 
     * @param baseCurrency la devise de base.
     * @param timestamp le timestamp actuel.
     * @param rates une map contenant les devises et leurs taux de change.
     * @return List<ExchangeRate> une liste d'objets ExchangeRate formatés.
     * @see ExchangeRate
     */
    private List<ExchangeRate> createExchangeRates(String baseCurrency, String timestamp, Map<String, Number> rates) {
        List<ExchangeRate> formattedRates = new ArrayList<>();
        rates.forEach((currency, rate) -> {
            ExchangeRate exchangeRate = new ExchangeRate();
            exchangeRate.setBase(baseCurrency);
            exchangeRate.setCurrency(currency);
            exchangeRate.setRate(rate.doubleValue());
            exchangeRate.setTimestamp(timestamp);
            exchangeRate.setDate(ZonedDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            formattedRates.add(exchangeRate);
        });
        return formattedRates;
    }

    /**
     * Récupère les taux de change depuis l'API, les formate, les publie sur Kafka,
     * les stocke dans MongoDB et les indexe dans Elasticsearch.
     * 
     * @return un message indiquant le succès de l'opération.
     * 
     */
    public String fetchAndStoreExchangeRates() {
        logger.info("Début de la récupération des taux de change depuis l'API...");
        Map<String, Object> response = fetchExchangeRatesFromApi();

        List<ExchangeRate> formattedRates = formatExchangeRates(response);

        publishToKafka(formattedRates);
        saveToMongoDB(formattedRates);
        indexToElasticsearch(formattedRates);

        return "Données publiées, stockées dans MongoDB et indexées avec succès.";
    }

    /**
     * Récupère les taux de change depuis l'API.
     * 
     * @return la réponse brute de l'API sous forme de map.
     */
    private Map<String, Object> fetchExchangeRatesFromApi() {
        @SuppressWarnings("unchecked")
        Map<String, Object> response = restTemplate.getForObject(apiUrl, Map.class);

        if (response == null || !"success".equals(response.get("result"))) {
            throw new RuntimeException("Impossible de récupérer les données de l'API.");
        }

        logger.info("Données récupérées depuis l'API : {}", response);
        return response;
    }

    /**
     * Publie les taux de change sur Kafka.
     * 
     * @param rates la liste des taux de change à publier.
     */
    private void publishToKafka(List<ExchangeRate> rates) {
        logger.info("Publication des taux sur Kafka...");
        for (ExchangeRate rate : rates) {
            try {
                String rateJson = objectMapper.writeValueAsString(rate);
                kafkaRepository.sendMessageToDefaultTopic(rateJson);
                logger.info("Publication du taux : {}", rate);
            } catch (JsonProcessingException e) {
                logger.error("Erreur lors de la conversion en JSON : {}", e.getMessage());
            }
        }
        logger.info("Publication sur Kafka terminée.");
    }

    /**
     * Stocke les taux de change dans MongoDB.
     * 
     * @param rates la liste des taux de change à stocker.
     */
    private void saveToMongoDB(List<ExchangeRate> rates) {
        logger.info("Stockage des taux dans MongoDB...");
        mongoDBRepository.saveAll(rates);
        logger.info("Stockage dans MongoDB terminé.");
    }

    /**
     * Indexe les taux de change dans Elasticsearch.
     * 
     * @param rates la liste des taux de change à indexer.
     */
    private void indexToElasticsearch(List<ExchangeRate> rates) {
        logger.info("Indexation des taux dans Elasticsearch...");
        elasticsearchRepository.indexAll(rates);
        logger.info("Indexation dans Elasticsearch terminée.");
    }
}
