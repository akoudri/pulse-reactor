package fr.janus.pulse.mvc.health;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration des upstreams interrogés par le fan-out bloquant (jumeau du réactif).
 *
 * @param baseUrl   URL de base (upstream-sim : {@code http://localhost:8089})
 * @param endpoints liste des upstreams (nom + chemin {@code /health/...})
 */
@ConfigurationProperties("pulse.upstream")
public record PulseUpstreamProperties(String baseUrl, List<Endpoint> endpoints) {

    public record Endpoint(String name, String path) {
    }
}
