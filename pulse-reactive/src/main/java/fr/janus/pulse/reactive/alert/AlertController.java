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
        return service.countByMetric();
    }

    /**
     * « Mes alertes » (lab J4-2 A) : les alertes créées par l'utilisateur authentifié. Le
     * principal est lu via {@code ReactiveSecurityContextHolder} dans le service — preuve que le
     * {@code SecurityContext} traverse le pipeline. Chemin littéral {@code /mine} : il prime sur
     * {@code /{id}}, pas de collision de routage.
     */
    @GetMapping("/mine")
    public Flux<Alert> mine() {
        return service.mine();
    }

    @GetMapping("/{id}")
    public Mono<ResponseEntity<Alert>> byId(@PathVariable String id) {
        // doFirst : le log s'exécute à la souscription (dans le pipeline), pas à l'assemblage —
        // le Context est alors établi et le traceId est dans le MDC (propagation automatique).
        return service.byId(id)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build())
                .doFirst(() -> log.info("entrée contrôleur GET /api/alerts/{}", id));
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<Alert> create(@Valid @RequestBody AlertRule rule) {
        return service.create(rule);
    }
}
