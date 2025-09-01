package com.learn.kafka.repository;

import com.learn.kafka.model.ExchangeRate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Repository pour interagir avec MongoDB.
 * Fournit des méthodes pour insérer des données dans la base de données MongoDB.
 */
@Repository
public class MongoDBRepository {


    /**
     * Repository pour interagir avec MongoDB pour les objets ExchangeRate.
     * Hérite de MongoRepository pour fournir des méthodes CRUD standard.
     */
    @Autowired
    private ExchangeRateRepository exchangeRateRepository;

    /**
     * Insère une liste d'objets ExchangeRate dans MongoDB.
     *
     * @param rates la liste des taux de change à insérer.
     */
    public void saveAll(List<ExchangeRate> rates) {
        exchangeRateRepository.insert(rates);
    }
}
