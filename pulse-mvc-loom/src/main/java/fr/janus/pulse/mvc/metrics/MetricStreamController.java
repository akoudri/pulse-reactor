package fr.janus.pulse.mvc.metrics;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * Même endpoint que le réactif : {@code GET /api/metrics/stream} en {@code text/event-stream},
 * via {@link SseEmitter} (l'API SSE de Spring MVC).
 */
@RestController
@RequestMapping("/api/metrics")
public class MetricStreamController {

    private final MetricBroadcaster broadcaster;

    public MetricStreamController(MetricBroadcaster broadcaster) {
        this.broadcaster = broadcaster;
    }

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream() {
        // TODO : enregistrer un nouvel abonné SSE (broadcaster.subscribe()).
    }
}
