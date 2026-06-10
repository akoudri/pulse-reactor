package fr.janus.pulse.reactive.metrics;

import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import fr.janus.pulse.common.EnrichedSample;
import fr.janus.pulse.reactive.ingestion.MetricIngestion;
import reactor.core.publisher.Flux;

/**
 * Pousse en temps réel vers le navigateur, en {@code text/event-stream}, le flux issu de
 * <strong>Kafka</strong> : {@code @KafkaListener → Sinks → normalize → enrich} (lab J3-2 B).
 * On retourne le {@link Flux} de {@link ServerSentEvent} au framework — pas de {@code block()}
 * ni de {@code subscribe()}.
 *
 * <p>Backpressure de bout en bout : si un client SSE est lent, WebFlux ne tire pas plus vite
 * que ce qu'il consomme ; la demande remonte jusqu'au pont {@link MetricStream} (borné), qui
 * applique alors sa stratégie d'overflow et déclenche, en amont, la pause de la consommation
 * Kafka. Aucune file non bornée ne se forme.
 */
@RestController
@RequestMapping("/api/metrics")
public class MetricStreamController {

    private final MetricIngestion ingestion;

    public MetricStreamController(MetricIngestion ingestion) {
        this.ingestion = ingestion;
    }

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<EnrichedSample>> stream() {
        return ingestion.processed()
                .map(sample -> ServerSentEvent.<EnrichedSample>builder()
                        .event("metric")
                        .data(sample)
                        .build());
    }
}
