package fr.janus.pulse.reactive.metrics;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import fr.janus.pulse.common.MetricSample;
import jakarta.annotation.PreDestroy;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

/**
 * Simulateur d'agents : pousse des échantillons synthétiques dans le {@link MetricStream}
 * à intervalle régulier, pour alimenter le flux SSE en l'absence de vrais agents.
 *
 * <p>Désactivable via {@code pulse.simulator.enabled=false} (utilisé par les tests qui
 * pilotent eux-mêmes les émissions).
 */
@Component
public class MetricSimulator {

    private static final Duration PERIOD = Duration.ofMillis(500);

    private final MetricStream stream;
    private final boolean enabled;
    private final AtomicLong sequence = new AtomicLong();

    private Disposable driver;

    public MetricSimulator(MetricStream stream,
                           @Value("${pulse.simulator.enabled:true}") boolean enabled) {
        this.stream = stream;
        this.enabled = enabled;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        if (!enabled) {
            return;
        }
        // Driver d'une source hot : on souscrit un interval pour pousser dans le sink.
        // Ce n'est pas un subscribe « sauvage » de contrôleur — c'est le générateur
        // d'événements, dont on conserve le Disposable pour l'arrêter au shutdown.
        this.driver = Flux.interval(PERIOD)
                .map(this::syntheticSample)
                .subscribe(stream::emit);
    }

    @PreDestroy
    public void stop() {
        if (driver != null) {
            driver.dispose();
        }
    }

    private MetricSample syntheticSample(long tick) {
        long n = sequence.incrementAndGet();
        double value = n % 100;
        return new MetricSample("agent-sim", "pulse.cpu.load", value, Instant.now());
    }
}
