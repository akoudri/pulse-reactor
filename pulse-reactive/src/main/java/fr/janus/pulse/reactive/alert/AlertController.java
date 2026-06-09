package fr.janus.pulse.reactive.alert;

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

    private final AlertService service;

    public AlertController(AlertService service) {
        this.service = service;
    }

    @GetMapping
    public Flux<Alert> list() {
        return service.all();
    }

    @GetMapping("/{id}")
    public Mono<ResponseEntity<Alert>> byId(@PathVariable String id) {
        return service.byId(id)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<Alert> create(@Valid @RequestBody AlertRule rule) {
        return service.create(rule);
    }
}
