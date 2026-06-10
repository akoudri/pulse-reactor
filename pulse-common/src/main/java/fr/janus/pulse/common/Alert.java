package fr.janus.pulse.common;

import java.time.Instant;

/**
 * Alerte créée à partir d'une {@link AlertRule}. Ressource exposée à l'identique par les
 * deux jumeaux ({@code pulse-reactive} et {@code pulse-mvc-loom}).
 *
 * @param id         identifiant unique attribué à la création
 * @param metricName métrique surveillée (repris de la règle)
 * @param threshold  seuil de déclenchement (repris de la règle)
 * @param severity   sévérité (reprise de la règle)
 * @param createdAt  instant de création
 */
public record Alert(String id, String metricName, double threshold, Severity severity, Instant createdAt) {

    /** Crée une alerte à partir d'une règle, en lui attribuant un id et un horodatage. */
    public static Alert from(String id, AlertRule rule, Instant createdAt) {
        return new Alert(id, rule.metricName(), rule.threshold(), rule.severity(), createdAt);
    }
}
