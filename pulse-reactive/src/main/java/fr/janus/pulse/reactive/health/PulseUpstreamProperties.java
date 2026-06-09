package fr.janus.pulse.reactive.health;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration des upstreams interrogés par le fan-out de santé.
 *
 * @param baseUrl   URL de base (upstream-sim : {@code http://localhost:8089})
 * @param endpoints liste des upstreams (nom + chemin {@code /health/...})
 */
@ConfigurationProperties("pulse.upstream")
public record PulseUpstreamProperties(String baseUrl, List<Endpoint> endpoints) {

    /** Un upstream à sonder. */
    public record Endpoint(String name, String path) {
    }
}
