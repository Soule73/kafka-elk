package com.learn.kafka.config;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponseInterceptor;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import com.fasterxml.jackson.databind.ObjectMapper;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;

@Configuration
public class ElasticsearchConfig {

    @Value("${elasticsearch.host}")
    private String elasticsearchHost;

    @Value("${elasticsearch.port}")
    private int elasticsearchPort;

    @Bean
    ElasticsearchClient elasticsearchClient() {
        // Configurer RestClient avec un en-tête compatible Elasticsearch
        RestClientBuilder builder = RestClient.builder(new HttpHost(elasticsearchHost, elasticsearchPort));
        builder.setDefaultHeaders(new org.apache.http.Header[]{
                new org.apache.http.message.BasicHeader("Content-Type", "application/json")
        });

        // Désactiver la vérification stricte des en-têtes
        builder.setStrictDeprecationMode(false);

        // Ajouter un intercepteur pour inclure l'en-tête X-Elastic-Product dans la réponse
        builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.addInterceptorLast(
                (HttpResponseInterceptor) (response, _) -> 
                        response.addHeader("X-Elastic-Product", "Elasticsearch")
        ));

        RestClient restClient = builder.build();

        // Configurer le transport
        RestClientTransport transport = new RestClientTransport(
                restClient,
                new JacksonJsonpMapper(new ObjectMapper())
        );

        return new ElasticsearchClient(transport);
    }
}
