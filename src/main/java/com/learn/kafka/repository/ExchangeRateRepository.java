package com.learn.kafka.repository;

import com.learn.kafka.model.ExchangeRate;
import org.springframework.data.mongodb.repository.MongoRepository;

/**
 * Repository pour interagir avec MongoDB pour les objets ExchangeRate.
 * Hérite de MongoRepository pour fournir des méthodes CRUD standard.
 */
public interface ExchangeRateRepository extends MongoRepository<ExchangeRate, String> {
    // Hérite des méthodes CRUD de MongoRepository
}
