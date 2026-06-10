package fr.janus.pulse.common;

import java.util.List;

/**
 * Agrégat de santé des upstreams (résultat du fan-out de la Partie B).
 *
 * @param overall   état global dérivé des upstreams
 * @param upstreams détail par upstream
 */
public record AggregateHealth(HealthStatus overall, List<UpstreamHealth> upstreams) {
}
