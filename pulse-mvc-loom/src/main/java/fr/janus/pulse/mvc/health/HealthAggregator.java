package fr.janus.pulse.mvc.health;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import fr.janus.pulse.common.AggregateHealth;
import fr.janus.pulse.common.HealthStatus;
import fr.janus.pulse.common.UpstreamHealth;

/**
 * Fan-out de santé <strong>bloquant</strong> (jumeau du {@code HealthAggregator} réactif),
 * via {@link RestClient}. Même contrat de résultat ({@link AggregateHealth}).
 *
 * <p><b>Parallélisme</b> : {@link CompletableFuture} sur un executor
 * <em>virtual-thread-per-task</em> — un appel bloquant par thread virtuel. C'est l'argument
 * Loom : on parallélise comme le réactif sans réserver un thread plateforme par appel en
 * attente. (Alternative documentée : appels séquentiels, plus simples mais latence cumulée.)
 *
 * <p><b>Résilience</b> : timeout de lecture (request factory) + petite boucle de retry +
 * repli {@code DOWN}. On garde une boucle impérative <em>lisible</em>, fidèle au style du
 * jumeau ; en production on privilégierait le {@code @Retryable} natif de Spring Framework 7
 * (cf. CLAUDE.md) ou Resilience4j pour le circuit breaker.
 */
@Service
public class HealthAggregator {

    private static final int MAX_ATTEMPTS = 3;
    private static final Duration READ_TIMEOUT = Duration.ofMillis(800);

    private final RestClient client;
    private final List<PulseUpstreamProperties.Endpoint> endpoints;

    public HealthAggregator(PulseUpstreamProperties properties) {
        JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory();
        factory.setReadTimeout(READ_TIMEOUT); // timeout par appel
        this.client = RestClient.builder()
                .baseUrl(properties.baseUrl())
                .requestFactory(factory)
                .build();
        this.endpoints = properties.endpoints();
    }

    public AggregateHealth aggregate() {
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<CompletableFuture<UpstreamHealth>> futures = endpoints.stream()
                    .map(endpoint -> CompletableFuture.supplyAsync(() -> probe(endpoint), executor))
                    .toList();
            List<UpstreamHealth> results = futures.stream()
                    .map(CompletableFuture::join)
                    .toList();
            return combine(results);
        }
    }

    private UpstreamHealth probe(PulseUpstreamProperties.Endpoint endpoint) {
        long start = System.nanoTime();
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try {
                client.get().uri(endpoint.path()).retrieve().toBodilessEntity();
                long elapsedMs = (System.nanoTime() - start) / 1_000_000;
                return new UpstreamHealth(endpoint.name(), HealthStatus.UP, elapsedMs);
            } catch (RuntimeException ex) {
                // 5xx, timeout de lecture, connexion : on retente, puis on se replie.
                if (attempt == MAX_ATTEMPTS) {
                    return new UpstreamHealth(endpoint.name(), HealthStatus.DOWN, -1L);
                }
                sleepBackoff(attempt);
            }
        }
        return new UpstreamHealth(endpoint.name(), HealthStatus.DOWN, -1L); // inatteignable
    }

    private static void sleepBackoff(int attempt) {
        try {
            Thread.sleep(100L * attempt); // sur virtual thread : pas de thread plateforme gelé
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static AggregateHealth combine(List<UpstreamHealth> results) {
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
