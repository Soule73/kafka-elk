package com.learn.kafka.controller;

import com.learn.kafka.service.ExchangeRateService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController("exchangeRateController")
public class ExchangeRateController {

    @Autowired
    private ExchangeRateService exchangeRateService;

    @GetMapping("/api/fetch-exchange-rates")
    public String fetchAndStoreExchangeRates() {
        try {
            return exchangeRateService.fetchAndStoreExchangeRates();
        } catch (Exception e) {
            return "Erreur lors de la récupération et du stockage des taux de change : " + e.getMessage();
        }
    }
}
