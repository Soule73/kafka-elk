package com.learn.kafka.consumer;

import lombok.extern.log4j.Log4j;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class MessageConsumer {

    @KafkaListener(topics = "exchange-rates-topic", groupId = "${spring.kafka.consumer.group-id}")
    public void listen(String message) {
        log.info("Message reçu : {}", message);
        // Vous pouvez ajouter ici un traitement supplémentaire si nécessaire
    }

}