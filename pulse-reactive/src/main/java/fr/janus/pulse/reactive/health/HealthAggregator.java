package fr.janus.pulse.reactive.health;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;

import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import fr.janus.pulse.common.AggregateHealth;
import fr.janus.pulse.common.HealthStatus;
import fr.janus.pulse.common.UpstreamHealth;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

/**
 * Fan-out de santé : interroge les upstreams <strong>en parallèle</strong> via
 * {@link WebClient}, chaque appel étant protégé (timeout + retry + repli), puis agrège.
 */
@Service
public class HealthAggregator {

    /** Concurrence bornée du fan-out : on parallélise sans marteler les upstreams. */
    private static final int UPSTREAM_CONCURRENCY = 4;

    /** Timeout appliqué <em>par appel</em> (placé sous le retry → vaut pour chaque tentative). */
    private static final Duration PER_CALL_TIMEOUT = Duration.ofMillis(800);

    private final WebClient client;
    private final List<PulseUpstreamProperties.Endpoint> endpoints;

    public HealthAggregator(WebClient.Builder builder, PulseUpstreamProperties properties) {
        // TODO : construire le WebClient (baseUrl = upstream-sim) et récupérer les endpoints.
    }

    public Mono<AggregateHealth> aggregate() {
        // TODO : interroger les upstreams en parallèle (fan-out à concurrence bornée),
        //        collecter les résultats et les agréger en AggregateHealth.
    }

    private Mono<UpstreamHealth> probe(PulseUpstreamProperties.Endpoint endpoint) {
        // TODO : appeler l'upstream avec timeout + retry + repli (onErrorReturn DOWN).
    }

    private static AggregateHealth combine(List<UpstreamHealth> results) {
        // TODO : dériver l'état global (UP / DEGRADED / DOWN) à partir des upstreams.
    }
}
