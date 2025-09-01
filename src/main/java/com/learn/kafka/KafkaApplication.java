package com.learn.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Classe principale de l'application Kafka.
 * Cette classe démarre l'application Spring Boot.
 * Elle active également la planification des tâches.
 * 
 */
@SpringBootApplication
@EnableScheduling
public class KafkaApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaApplication.class, args);
    }
}
