package fr.janus.pulse.reactive.health;

import java.time.Duration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

/**
 * Beans de résilience (lab J3-2 C), créés <strong>explicitement</strong> — on n'ajoute pas le
 * starter Boot Resilience4j (auto-config non alignée Boot 4) : la config reste visible et testable.
 *
 * <p>Le circuit breaker est dimensionné pour s'<em>ouvrir vite et nettement</em> sous un upstream
 * durablement KO (fenêtre courte, seuil bas) : c'est ce qui rend le test d'ouverture déterministe.
 */
@Configuration(proxyBeanMethods = false)
class ResilienceConfig {

    /**
     * Un registre fournit un circuit breaker <strong>par upstream</strong> (clé = nom) : chaque
     * dépendance a son propre état, une seule défaillante n'ouvre pas les autres.
     */
    @Bean
    CircuitBreakerRegistry circuitBreakerRegistry() {
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(5)
                // En deçà de ce nombre d'appels, le taux d'échec n'est pas évalué : un unique
                // 503 récupéré par retry ne fait pas basculer le circuit.
                .minimumNumberOfCalls(3)
                .failureRateThreshold(50.0f)
                // Reste ouvert un temps avant de tester la reprise (half-open).
                .waitDurationInOpenState(Duration.ofSeconds(5))
                .permittedNumberOfCallsInHalfOpenState(2)
                .build();
        return CircuitBreakerRegistry.of(config);
    }

    /**
     * Bulkhead (sémaphore) : borne le nombre d'appels concurrents vers les upstreams, isolant
     * la ressource. C'est la variante « explicite » ; la borne native légère reste l'argument de
     * concurrence de {@code flatMap} dans {@link HealthAggregator}. On positionne les deux.
     */
    @Bean
    BulkheadRegistry bulkheadRegistry() {
        BulkheadConfig config = BulkheadConfig.custom()
                .maxConcurrentCalls(8)
                // File d'attente nulle : au-delà des permis, on rejette tout de suite
                // (BulkheadFullException → repli) plutôt que de faire patienter.
                .maxWaitDuration(Duration.ZERO)
                .build();
        return BulkheadRegistry.of(config);
    }
}
