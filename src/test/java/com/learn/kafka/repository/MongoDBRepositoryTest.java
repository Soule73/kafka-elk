package com.learn.kafka.repository;

import com.learn.kafka.model.ExchangeRate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;

import static org.mockito.Mockito.*;

class MongoDBRepositoryTest {

    @Mock
    private ExchangeRateRepository exchangeRateRepository;

    @InjectMocks
    private MongoDBRepository mongoDBRepository;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    /**
     * Teste la méthode `saveAll` pour s'assurer qu'elle appelle correctement
     * la méthode `insert` du repository MongoDB avec une liste d'objets `ExchangeRate`.
     */
    @Test
    void testSaveAll() {
        // Préparer un objet ExchangeRate simulé
        ExchangeRate rate = new ExchangeRate();
        rate.setBase("USD");
        rate.setCurrency("EUR");
        rate.setRate(0.88);

        // Appeler la méthode à tester
        mongoDBRepository.saveAll(List.of(rate));

        // Vérifier que la méthode `insert` du repository a été appelée une fois avec une liste
        verify(exchangeRateRepository, times(1)).insert(anyList());
    }
}
