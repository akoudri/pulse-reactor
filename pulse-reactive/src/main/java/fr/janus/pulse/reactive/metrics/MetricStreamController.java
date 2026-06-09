package fr.janus.pulse.reactive.metrics;

import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import fr.janus.pulse.common.MetricSample;
import reactor.core.publisher.Flux;

/**
 * Pousse les métriques en temps réel vers le navigateur en {@code text/event-stream}.
 * On retourne le {@link Flux} de {@link ServerSentEvent} au framework — pas de
 * {@code block()} ni de {@code subscribe()}.
 */
@RestController
@RequestMapping("/api/metrics")
public class MetricStreamController {

    private final MetricStream metricStream;

    public MetricStreamController(MetricStream metricStream) {
        this.metricStream = metricStream;
    }

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<MetricSample>> stream() {
        // TODO : mapper le flux de métriques en Flux<ServerSentEvent<MetricSample>>.
    }
}
