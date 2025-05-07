package com.learn.kafka.scheduler;

import com.learn.kafka.service.ExchangeRateService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component
public class ExchangeRateScheduler {

    private static final Logger logger = LoggerFactory.getLogger(ExchangeRateScheduler.class);

    @Autowired
    private ExchangeRateService exchangeRateService;

    // Planifier la tâche pour s'exécuter toutes les 60 secondes
    @Scheduled(fixedRate = 60000)
    public void fetchExchangeRates() {
        logger.info("Début de l'exécution du scheduler pour récupérer les taux de change...");
        try {
            String result = exchangeRateService.fetchAndStoreExchangeRates();
            logger.info("Scheduler terminé avec succès : {}", result);
        } catch (Exception e) {
            logger.error("Erreur lors de l'exécution du scheduler : {}", e.getMessage());
        }
    }
}
