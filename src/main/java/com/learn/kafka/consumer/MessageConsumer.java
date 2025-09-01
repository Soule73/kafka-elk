package com.learn.kafka.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Composant pour consommer les messages depuis Kafka.
 * Écoute les messages publiés sur un topic Kafka et les traite.
 */
@Component
public class MessageConsumer {

    /**
     * Méthode pour consommer les messages depuis le topic Kafka configuré.
     *
     * @param message le message consommé depuis Kafka.
     */
    @KafkaListener(topics = "${spring.kafka.template.default-topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void consume(String message) {
        System.out.println("Message consommé : " + message);
    }
}