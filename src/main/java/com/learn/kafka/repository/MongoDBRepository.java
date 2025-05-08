package com.learn.kafka.repository;

import com.learn.kafka.model.ExchangeRate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class MongoDBRepository {

    @Autowired
    private ExchangeRateRepository exchangeRateRepository;

    public void saveAll(List<ExchangeRate> rates) {
        exchangeRateRepository.insert(rates);
    }
}
