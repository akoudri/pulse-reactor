package fr.janus.pulse.mvc.health;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import fr.janus.pulse.common.AggregateHealth;

/** Même endpoint que le réactif : {@code GET /api/health/aggregate}, version bloquante. */
@RestController
@RequestMapping("/api/health")
public class HealthController {

    private final HealthAggregator aggregator;

    public HealthController(HealthAggregator aggregator) {
        this.aggregator = aggregator;
    }

    @GetMapping("/aggregate")
    public AggregateHealth aggregate() {
        // TODO : retourner l'agrégat de santé (aggregator.aggregate()).
    }
}
