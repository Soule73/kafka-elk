package com.learn.kafka.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import co.elastic.clients.elasticsearch.ElasticsearchClient;

@Component
public class MessageProducer {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private final ElasticsearchClient elasticsearchClient;

    public MessageProducer(ElasticsearchClient elasticsearchClient) {
        this.elasticsearchClient = elasticsearchClient;
    }

    public void sendMessage(String topic, String message) {
        kafkaTemplate.send(topic, message);
    }

    public void sendMessage(String message) {
        sendMessage("exchange-rates-topic", message);
    }

    public void indexDocument(String index, String id, Object document) {
        try {
            elasticsearchClient.index(i -> i
                .index(index)
                .id(id)
                .document(document)
            );
        } catch (Exception e) {
            throw new RuntimeException("Erreur lors de l'indexation dans Elasticsearch : " + e.getMessage());
        }
    }
}