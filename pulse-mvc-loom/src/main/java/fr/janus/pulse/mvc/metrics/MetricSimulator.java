package fr.janus.pulse.mvc.metrics;

import java.time.Instant;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import fr.janus.pulse.common.MetricSample;
import jakarta.annotation.PreDestroy;

/**
 * Simulateur d'agents, version impérative : une boucle sur un <strong>virtual thread</strong>
 * pousse des échantillons synthétiques au {@link MetricBroadcaster} toutes les 500 ms.
 * Désactivable via {@code pulse.simulator.enabled=false}.
 */
@Component
public class MetricSimulator {

    private static final long PERIOD_MS = 500L;

    private final MetricBroadcaster broadcaster;
    private final boolean enabled;
    private volatile boolean running = true;

    public MetricSimulator(MetricBroadcaster broadcaster,
                           @Value("${pulse.simulator.enabled:true}") boolean enabled) {
        this.broadcaster = broadcaster;
        this.enabled = enabled;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        // TODO : si activé, démarrer une boucle d'émission sur un virtual thread dédié.
    }

    private void loop() {
        // TODO : pousser périodiquement un échantillon synthétique au broadcaster.
    }

    @PreDestroy
    public void stop() {
        // TODO : arrêter la boucle d'émission.
    }
}
