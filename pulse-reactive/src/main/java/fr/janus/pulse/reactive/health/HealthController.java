package fr.janus.pulse.reactive.health;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import fr.janus.pulse.common.AggregateHealth;
import reactor.core.publisher.Mono;

/** Expose l'agrégat de santé du fan-out sous {@code /api/health/aggregate}. */
@RestController
@RequestMapping("/api/health")
public class HealthController {

    private final HealthAggregator aggregator;

    public HealthController(HealthAggregator aggregator) {
        this.aggregator = aggregator;
    }

    @GetMapping("/aggregate")
    public Mono<AggregateHealth> aggregate() {
        // TODO : retourner l'agrégat de santé (aggregator.aggregate()).
    }
}
