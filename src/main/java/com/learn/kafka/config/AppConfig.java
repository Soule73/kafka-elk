package com.learn.kafka.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

/**
 * Configuration de l'application.
 * Cette classe contient les beans nécessaires à l'application.
 */
@Configuration
public class AppConfig {

    /**
     * Définit un bean RestTemplate pour effectuer des appels HTTP externes.
     * 
     * @return une instance de RestTemplate.
     */
    @Bean
    RestTemplate restTemplate() {
        return new RestTemplate();
    }
}
