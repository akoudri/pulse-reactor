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
        // TODO : construire le RestClient (baseUrl + timeout de lecture) et récupérer les endpoints.
    }

    public AggregateHealth aggregate() {
        // TODO : interroger les upstreams en parallèle (CompletableFuture sur executor virtuel
        //        OU séquentiel — à documenter), collecter et agréger en AggregateHealth.
    }

    private UpstreamHealth probe(PulseUpstreamProperties.Endpoint endpoint) {
        // TODO : appel bloquant avec retry + repli DOWN en cas d'échec persistant.
    }

    private static void sleepBackoff(int attempt) {
        // TODO : attente entre deux tentatives (backoff).
    }

    private static AggregateHealth combine(List<UpstreamHealth> results) {
        // TODO : dériver l'état global (UP / DEGRADED / DOWN) à partir des upstreams.
    }
}
