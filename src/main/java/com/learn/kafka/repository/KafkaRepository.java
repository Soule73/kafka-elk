package com.learn.kafka.repository;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Repository;

/**
 * Repository pour interagir avec Kafka.
 * Fournit des méthodes pour envoyer des messages à un topic Kafka.
 */
@Repository
public class KafkaRepository {

    /*
     * KafkaTemplate est utilisé pour envoyer des messages à un topic Kafka.
     * Il est injecté par Spring lors de l'initialisation de la classe.
     */
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    /*
     * Nom du topic par défaut pour envoyer les messages Kafka.
     * Il est injecté à partir du fichier de configuration (application.properties ou application.yml).
     */
    @Value("${spring.kafka.template.default-topic}")
    private String defaultTopic;

    /**
     * Envoie un message à un topic Kafka spécifié.
     *
     * @param topic   le nom du topic Kafka.
     * @param message le message à envoyer.
     */
    public void sendMessage(String topic, String message) {
        kafkaTemplate.send(topic, message);
    }

    /**
     * Envoie un message au topic Kafka par défaut.
     *
     * @param message le message à envoyer.
     */
    public void sendMessageToDefaultTopic(String message) {
        kafkaTemplate.send(defaultTopic, message);
    }
}
