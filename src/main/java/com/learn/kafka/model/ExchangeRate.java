package com.learn.kafka.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "exchange_rates")
public class ExchangeRate {

    @Id
    private String id;
    private String base;
    private String currency;
    private double rate;
    private String timestamp;
    private String date;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getBase() {
        return base;
    }

    public void setBase(String base) {
        this.base = base;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public double getRate() {
        return rate;
    }

    public void setRate(double rate) {
        this.rate = rate;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    @Override
    public String toString() {
        return "ExchangeRate{" +
                "id='" + id + '\'' +
                ", base='" + base + '\'' +
                ", currency='" + currency + '\'' +
                ", rate=" + rate +
                ", timestamp='" + timestamp + '\'' +
                ", date='" + date + '\'' +
                '}';
    }
}