package fr.janus.pulse.reactive.ingestion;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration de l'ingestion Kafka.
 *
 * @param topic     topic source des métriques (consommé par le pont, produit par le simulateur)
 * @param simulator simulateur d'agents (producteur Kafka de démo)
 */
@ConfigurationProperties("pulse.ingestion")
public record PulseIngestionProperties(String topic, Simulator simulator) {

    /** @param enabled active le simulateur d'agents (désactivé en test). */
    public record Simulator(boolean enabled) {
    }
}
