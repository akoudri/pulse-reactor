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
        this.client = builder.baseUrl(properties.baseUrl()).build();
        this.endpoints = properties.endpoints();
    }

    public Mono<AggregateHealth> aggregate() {
        // flatMap : l'ordre des réponses n'importe pas, on veut le parallélisme.
        // concatMap sérialiserait les appels (latence cumulée). Concurrence bornée explicite.
        return Flux.fromIterable(endpoints)
                .flatMap(this::probe, UPSTREAM_CONCURRENCY)
                .collectList()
                .map(HealthAggregator::combine);
    }

    private Mono<UpstreamHealth> probe(PulseUpstreamProperties.Endpoint endpoint) {
        return client.get().uri(endpoint.path())
                .retrieve()
                .toBodilessEntity()
                .timeout(PER_CALL_TIMEOUT)
                .retryWhen(Retry.backoff(2, Duration.ofMillis(100)))
                .elapsed() // (latenceMs, réponse)
                .map(timed -> new UpstreamHealth(endpoint.name(), HealthStatus.UP, timed.getT1()))
                // Repli : un upstream KO (timeout / 5xx après retries) ne casse pas l'agrégat.
                .onErrorReturn(new UpstreamHealth(endpoint.name(), HealthStatus.DOWN, -1L));
    }

    private static AggregateHealth combine(List<UpstreamHealth> results) {
        // Tri par nom : sortie déterministe (utile aux tests et à la lecture).
        List<UpstreamHealth> sorted = results.stream()
                .sorted(Comparator.comparing(UpstreamHealth::name))
                .toList();
        boolean allDown = sorted.stream().allMatch(u -> u.status() == HealthStatus.DOWN);
        boolean anyDown = sorted.stream().anyMatch(u -> u.status() == HealthStatus.DOWN);
        HealthStatus overall = allDown ? HealthStatus.DOWN
                : anyDown ? HealthStatus.DEGRADED
                : HealthStatus.UP;
        return new AggregateHealth(overall, sorted);
    }
}
