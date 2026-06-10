package fr.janus.pulse.common;

import java.util.List;

/**
 * Agrégat de santé des upstreams (résultat du fan-out de la Partie B).
 *
 * @param overall   état global déduit du détail (UP / DEGRADED / DOWN)
 * @param upstreams détail par upstream, trié pour une sortie déterministe
 */
public record AggregateHealth(HealthStatus overall, List<UpstreamHealth> upstreams) {
}
