package fr.janus.pulse.common;

import java.time.Instant;

/**
 * Échantillon brut de métrique émis par un agent de télémétrie.
 *
 * @param agentId identifiant de l'agent émetteur
 * @param name    nom de la métrique (préfixé {@code "pulse."} après normalisation)
 * @param value   valeur mesurée
 * @param at      instant de la mesure
 */
public record MetricSample() {
}
