package fr.janus.pulse.common;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/**
 * Payload de création d'une alerte (contrat d'API, validé). Une règle déclenche une
 * alerte lorsque la métrique {@code metricName} franchit {@code threshold}.
 *
 * @param metricName nom de la métrique surveillée (non vide)
 * @param threshold  seuil de déclenchement
 * @param severity   sévérité de l'alerte produite
 */
public record AlertRule(
        @NotBlank String metricName,
        double threshold,
        @NotNull Severity severity) {
}
