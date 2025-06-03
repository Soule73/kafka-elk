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

/**
 * Configuration pour le client Elasticsearch.
 * Fournit un bean ElasticsearchClient configuré pour interagir avec un cluster
 * Elasticsearch.
 */
@Configuration
public class ElasticsearchConfig {

        /**
         * Adresse de l'hôte Elasticsearch.
         */
        @Value("${elasticsearch.host}")
        private String elasticsearchHost;

        /**
         * Port de l'hôte Elasticsearch.
         */
        @Value("${elasticsearch.port}")
        private int elasticsearchPort;

        /**
         * Crée et configure un bean ElasticsearchClient.
         * 
         * @return une instance configurée de ElasticsearchClient.
         */
        @Bean
        ElasticsearchClient elasticsearchClient() {

                RestClientBuilder builder = RestClient.builder(new HttpHost(elasticsearchHost, elasticsearchPort));
                builder.setDefaultHeaders(new org.apache.http.Header[] {
                                new org.apache.http.message.BasicHeader("Content-Type", "application/json")
                });

                builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.addInterceptorLast(
                                (HttpResponseInterceptor) (response, context) -> response.addHeader("X-Elastic-Product",
                                                "Elasticsearch")));

                RestClient restClient = builder.build();

                RestClientTransport transport = new RestClientTransport(
                                restClient,
                                new JacksonJsonpMapper(new ObjectMapper()));

                return new ElasticsearchClient(transport);
        }
}
