package fr.janus.pulse.common;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

/**
 * Payload de création d'une alerte (contrat d'API, validé). Une règle déclenche une
 * alerte lorsque la métrique surveillée franchit un seuil.
 *
 * @param metricName nom de la métrique surveillée (obligatoire)
 * @param threshold  seuil de déclenchement (strictement positif)
 * @param severity   sévérité de l'alerte (obligatoire)
 */
public record AlertRule(
        @NotBlank String metricName,
        @Positive double threshold,
        @NotNull Severity severity) {
}
