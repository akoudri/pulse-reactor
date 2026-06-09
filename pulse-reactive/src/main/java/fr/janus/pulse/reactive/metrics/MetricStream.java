package fr.janus.pulse.reactive.metrics;

import org.springframework.stereotype.Component;

import fr.janus.pulse.common.MetricSample;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

/**
 * Source <em>hot</em> programmatique des métriques, via l'API {@link Sinks} (jamais un
 * {@code Processor} déprécié). Multicast : chaque abonné SSE reçoit le flux en direct.
 *
 * <p>{@code onBackpressureBuffer} : si un consommateur lent ne suit pas, on bufferise
 * plutôt que de pousser au-delà de sa demande (cf. backpressure du lab J2-1).
 */
@Component
public class MetricStream {

    private final Sinks.Many<MetricSample> sink = Sinks.many().multicast().onBackpressureBuffer();

    /** Flux à exposer aux abonnés (contrôleur SSE). */
    public Flux<MetricSample> stream() {
        // TODO : exposer le sink sous forme de Flux.
    }

    /** Pousse un échantillon dans le flux. Retourne le résultat d'émission du sink. */
    public Sinks.EmitResult emit(MetricSample sample) {
        // TODO : émettre l'échantillon dans le sink.
    }
}
