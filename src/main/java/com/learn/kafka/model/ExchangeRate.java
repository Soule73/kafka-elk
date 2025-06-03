package com.learn.kafka.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * Modèle représentant un taux de change.
 * Cette classe est utilisée pour stocker les données des taux de change dans MongoDB.
 */
@Document(collection = "exchange_rates")
public class ExchangeRate {

    /**
     * Identifiant unique du document dans MongoDB.
     * Il est annoté avec @Id pour indiquer qu'il s'agit de la clé primaire.
     */
    @Id
    private String id;

    /**
     * Devise de base pour le taux de change.
     * Par exemple, "USD" pour le dollar américain.
     */
    private String base;

    /**
     * Devise cible pour le taux de change.
     * Par exemple, "EUR" pour l'euro.
     */
    private String currency;

    /**
     * Taux de change entre la devise de base et la devise cible.
     * Par exemple, 0.85 pour un taux de change de 1 USD à 0.85 EUR.
     */
    private double rate;

    /**
     * Timestamp de la récupération des données.
     * Représente le moment où les données ont été récupérées.
     */
    private String timestamp;

    /**
     * Date de récupération des données.
     * Représente la date à laquelle les données ont été récupérées.
     */
    private String date; 

    /**
     * Retourne l'identifiant unique du document.
     *
     * @return l'identifiant unique.
     */
    public String getId() {
        return id;
    }

    /**
     * Définit l'identifiant unique du document.
     *
     * @param id l'identifiant unique.
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * Retourne la devise de base.
     *
     * @return la devise de base.
     */
    public String getBase() {
        return base;
    }

    /**
     * Définit la devise de base.
     *
     * @param base la devise de base.
     */
    public void setBase(String base) {
        this.base = base;
    }

    /**
     * Retourne la devise cible.
     *
     * @return la devise cible.
     */
    public String getCurrency() {
        return currency;
    }

    /**
     * Définit la devise cible.
     *
     * @param currency la devise cible.
     */
    public void setCurrency(String currency) {
        this.currency = currency;
    }

    /**
     * Retourne le taux de change.
     *
     * @return le taux de change.
     */
    public double getRate() {
        return rate;
    }

    /**
     * Définit le taux de change.
     *
     * @param rate le taux de change.
     */
    public void setRate(double rate) {
        this.rate = rate;
    }

    /**
     * Retourne le timestamp de la récupération des données.
     *
     * @return le timestamp.
     */
    public String getTimestamp() {
        return timestamp;
    }

    /**
     * Définit le timestamp de la récupération des données.
     *
     * @param timestamp le timestamp.
     */
    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Retourne la date de récupération des données.
     *
     * @return la date.
     */
    public String getDate() {
        return date;
    }

    /**
     * Définit la date de récupération des données.
     *
     * @param date la date.
     */
    public void setDate(String date) {
        this.date = date;
    }

    /**
     * Retourne une représentation sous forme de chaîne de l'objet ExchangeRate.
     *
     * @return une chaîne représentant l'objet ExchangeRate.
     */
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