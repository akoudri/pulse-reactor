package fr.janus.pulse.reactive.alert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import fr.janus.pulse.common.Alert;
import fr.janus.pulse.common.AlertRule;
import jakarta.validation.Valid;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * CRUD d'alertes en <strong>style annoté</strong> sous {@code /api/alerts}. On retourne les
 * Publishers au framework (jamais de {@code block()} / {@code subscribe()} ici).
 */
@RestController
@RequestMapping("/api/alerts")
public class AlertController {

    private static final Logger log = LoggerFactory.getLogger(AlertController.class);

    private final AlertService service;

    public AlertController(AlertService service) {
        this.service = service;
    }

    @GetMapping
    public Flux<Alert> list() {
        return service.all();
    }

    /**
     * Agrégat alertes/métrique servi par une requête {@code DatabaseClient} (GROUP BY).
     * Le chemin littéral {@code /by-metric} prime sur {@code /{id}} (plus spécifique), donc
     * pas de collision de routage.
     */
    @GetMapping("/by-metric")
    public Flux<MetricAlertCount> byMetric() {
        // TODO: déléguer à l'agrégat countByMetric() du service
        return null;
    }

    @GetMapping("/{id}")
    public Mono<ResponseEntity<Alert>> byId(@PathVariable String id) {
        // TODO: déléguer au service ; 200 si présent, 404 sinon
        // TODO: logguer l'entrée du contrôleur à la souscription (doFirst) pour la corrélation traceId
        return null;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<Alert> create(@Valid @RequestBody AlertRule rule) {
        return service.create(rule);
    }
}
