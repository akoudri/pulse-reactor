package fr.janus.pulse.reactive.health;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;

import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import fr.janus.pulse.common.AggregateHealth;
import fr.janus.pulse.common.HealthStatus;
import fr.janus.pulse.common.UpstreamHealth;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.reactor.bulkhead.operator.BulkheadOperator;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

/**
 * Fan-out de santé : interroge les upstreams <strong>en parallèle</strong> via {@link WebClient},
 * chaque appel protégé par une pile de résilience (lab J3-2 C), puis agrège.
 *
 * <p>Pile, du plus interne au plus externe (l'ordre des {@code transformDeferred} compte : le
 * dernier appliqué est souscrit en premier) :
 * <ol>
 *   <li><strong>Hedging</strong> sur l'appel <em>idempotent</em> (GET) :
 *       {@code Mono.firstWithSignal(call, call.delaySubscription(d))}. Une 2e tentative ne part
 *       que si la 1re traîne au-delà de {@link #HEDGE_DELAY} ; le premier signal gagne, l'autre
 *       est annulé. <em>Surcoût</em> : sur un upstream lent, on double la charge de cet appel
 *       (deux requêtes en vol). On l'accepte pour raboter la latence de queue, et
 *       <strong>uniquement</strong> sur un appel idempotent — sinon on risquerait des doublons.</li>
 *   <li>{@code timeout} par tentative, puis {@code retryWhen(backoff)} (récupère un 5xx passager).</li>
 *   <li><strong>Bulkhead</strong> : borne les appels concurrents (isolation de ressource).</li>
 *   <li><strong>Circuit breaker</strong> (le plus externe) : sous échecs répétés il s'ouvre et
 *       court-circuite l'appel ({@code CallNotPermittedException}) sans même tenter le réseau —
 *       on bascule alors directement en repli.</li>
 * </ol>
 * Le repli unique ({@code onErrorResume → DOWN}) couvre indifféremment timeout, 5xx après retries,
 * circuit ouvert et bulkhead plein : un upstream KO ne casse jamais l'agrégat.
 */
@Service
public class HealthAggregator {

    /** Concurrence bornée native du fan-out (variante légère, complémentaire du bulkhead R4J). */
    private static final int UPSTREAM_CONCURRENCY = 4;

    /** Timeout appliqué <em>par tentative</em> (sous le retry → vaut pour chaque essai). */
    private static final Duration PER_CALL_TIMEOUT = Duration.ofMillis(800);

    /** Délai avant la 2e tentative de hedging : assez court pour aider la queue, assez long
     *  pour ne pas doubler systématiquement la charge. */
    private static final Duration HEDGE_DELAY = Duration.ofMillis(300);

    private static final String BULKHEAD_NAME = "upstreams";

    private final WebClient client;
    private final List<PulseUpstreamProperties.Endpoint> endpoints;
    private final CircuitBreakerRegistry circuitBreakers;
    private final BulkheadRegistry bulkheads;
    private final Timer aggregateTimer;

    public HealthAggregator(WebClient.Builder builder, PulseUpstreamProperties properties,
                            CircuitBreakerRegistry circuitBreakers, BulkheadRegistry bulkheads,
                            MeterRegistry meters) {
        this.client = builder.baseUrl(properties.baseUrl()).build();
        this.endpoints = properties.endpoints();
        this.circuitBreakers = circuitBreakers;
        this.bulkheads = bulkheads;
        this.aggregateTimer = Timer.builder("pulse.health.aggregate")
                .description("Durée d'une agrégation de santé (fan-out)")
                .register(meters);
    }

    public Mono<AggregateHealth> aggregate() {
        // Timer démarré à la souscription (defer), arrêté au terminal — sonde non bloquante.
        return Mono.defer(() -> {
            Timer.Sample sample = Timer.start();
            // flatMap : l'ordre des réponses n'importe pas, on veut le parallélisme. Concurrence bornée.
            return Flux.fromIterable(endpoints)
                    .flatMap(this::probe, UPSTREAM_CONCURRENCY)
                    .collectList()
                    .map(HealthAggregator::combine)
                    .doFinally(signal -> sample.stop(aggregateTimer));
        });
    }

    private Mono<UpstreamHealth> probe(PulseUpstreamProperties.Endpoint endpoint) {
        CircuitBreaker breaker = circuitBreakers.circuitBreaker(endpoint.name());
        Bulkhead bulkhead = bulkheads.bulkhead(BULKHEAD_NAME);

        Mono<?> call = client.get().uri(endpoint.path()).retrieve().toBodilessEntity();
        // Hedging : 2e tentative différée, le 1er signal (succès OU échec) gagne.
        Mono<?> hedged = Mono.firstWithSignal(call, call.delaySubscription(HEDGE_DELAY));

        return hedged
                .timeout(PER_CALL_TIMEOUT)
                .retryWhen(Retry.backoff(2, Duration.ofMillis(100)))
                .transformDeferred(BulkheadOperator.of(bulkhead))
                .transformDeferred(CircuitBreakerOperator.of(breaker))
                .elapsed() // (latenceMs, réponse)
                .map(timed -> new UpstreamHealth(endpoint.name(), HealthStatus.UP, timed.getT1()))
                // Repli : timeout / 5xx après retries / circuit ouvert / bulkhead plein → DOWN.
                .onErrorResume(ex -> Mono.just(new UpstreamHealth(endpoint.name(), HealthStatus.DOWN, -1L)));
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
