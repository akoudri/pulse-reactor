package fr.janus.pulse.common;

import java.time.Instant;

/**
 * Alerte créée à partir d'une {@link AlertRule}. Ressource exposée à l'identique par les
 * deux jumeaux ({@code pulse-reactive} et {@code pulse-mvc-loom}).
 *
 * @param id         identifiant de l'alerte
 * @param metricName métrique surveillée
 * @param threshold  seuil de déclenchement
 * @param severity   sévérité
 * @param createdAt  instant de création
 */
public record Alert(String id, String metricName, double threshold, Severity severity, Instant createdAt) {

    /** Construit une alerte à partir d'une règle validée. */
    public static Alert from(String id, AlertRule rule, Instant createdAt) {
        return new Alert(id, rule.metricName(), rule.threshold(), rule.severity(), createdAt);
    }
}
