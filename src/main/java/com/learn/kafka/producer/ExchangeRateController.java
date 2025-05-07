package com.learn.kafka.producer;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RestController
public class ExchangeRateController {

    @Autowired
    private ExchangeRateService exchangeRateService;

    @GetMapping("/api/fetch-exchange-rates")
    public ResponseEntity<String> fetchExchangeRates() {
        try {
            String documentId = exchangeRateService.fetchAndStoreExchangeRates();
            return ResponseEntity.ok("Données récupérées et stockées avec succès. ID du document : " + documentId);
        } catch (IOException e) {
            return ResponseEntity.status(500).body("Erreur lors de la récupération ou du stockage des données : " + e.getMessage());
        }
    }
}
