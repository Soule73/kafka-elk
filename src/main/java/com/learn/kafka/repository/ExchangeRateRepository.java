package com.learn.kafka.repository;

import com.learn.kafka.model.ExchangeRate;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface ExchangeRateRepository extends MongoRepository<ExchangeRate, String> {
}
