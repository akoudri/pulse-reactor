package fr.janus.pulse.reactive.health;

import org.springframework.stereotype.Component;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Compte les <strong>ouvertures de circuit</strong> (lab J3-2 D) : un {@link Counter}
 * {@code pulse.circuit.open} incrémenté à chaque transition vers l'état {@code OPEN}, tous
 * upstreams confondus.
 *
 * <p>On s'abonne aux événements de chaque breaker du {@link CircuitBreakerRegistry} — ceux déjà
 * présents et, via {@code onEntryAdded}, ceux créés à la volée (un breaker par upstream est
 * instancié au premier appel). Le compteur est enregistré tôt → visible dès le démarrage sur
 * {@code /actuator/prometheus} (à zéro avant la première ouverture).
 */
@Component
class CircuitBreakerMetrics {

    CircuitBreakerMetrics(CircuitBreakerRegistry registry, MeterRegistry meters) {
        Counter opens = Counter.builder("pulse.circuit.open")
                .description("Nombre de transitions de circuit vers l'état OUVERT")
                .register(meters);
        registry.getAllCircuitBreakers().forEach(cb -> bind(cb, opens));
        registry.getEventPublisher().onEntryAdded(event -> bind(event.getAddedEntry(), opens));
    }

    private static void bind(CircuitBreaker breaker, Counter opens) {
        breaker.getEventPublisher().onStateTransition(event -> {
            if (event.getStateTransition().getToState() == CircuitBreaker.State.OPEN) {
                opens.increment();
            }
        });
    }
}
