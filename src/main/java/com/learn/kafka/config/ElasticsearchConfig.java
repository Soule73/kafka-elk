package com.learn.kafka.config;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import com.fasterxml.jackson.databind.ObjectMapper;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;

@Configuration
public class ElasticsearchConfig {

    @Bean
    public ElasticsearchClient elasticsearchClient() {
        // Configurer RestClient avec un en-tête compatible Elasticsearch 7.x
        RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200));
        builder.setDefaultHeaders(new org.apache.http.Header[]{
            // Forcer le Content-Type à "application/json"
                new org.apache.http.message.BasicHeader("Content-Type", "application/json")
        });

        RestClient restClient = builder.build();

        // Configurer le transport avec JacksonJsonpMapper
        RestClientTransport transport = new RestClientTransport(
                restClient,
                new JacksonJsonpMapper(new ObjectMapper())
        );

        // Retourner le client Elasticsearch
        return new ElasticsearchClient(transport);
    }
}
