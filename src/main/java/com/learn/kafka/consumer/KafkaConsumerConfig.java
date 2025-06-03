package com.learn.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration pour le consommateur Kafka.
 * Définit les propriétés nécessaires pour consommer des messages depuis Kafka.
 */
@Configuration
public class KafkaConsumerConfig {

    /**
     * Adresse du serveur Kafka.
     */
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /**
     * ID du groupe de consommateurs Kafka.
     */
    @Value("${spring.kafka.consumer.group-id}")
    private String consumerGroupId;

    /**
     * Crée un ConsumerFactory pour configurer le consommateur Kafka.
     *
     * @return une instance de ConsumerFactory configurée.
     */
    @Bean
    ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    /**
     * Crée un ConcurrentKafkaListenerContainerFactory pour gérer les listeners Kafka.
     *
     * @return une instance de ConcurrentKafkaListenerContainerFactory configurée.
     */
    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }
}
