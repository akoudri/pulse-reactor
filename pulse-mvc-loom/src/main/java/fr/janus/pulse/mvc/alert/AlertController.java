package fr.janus.pulse.mvc.alert;

import java.util.List;
import java.util.Map;

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

/**
 * CRUD d'alertes, <strong>mêmes chemins et DTOs</strong> que le jumeau réactif
 * ({@code /api/alerts}), en style impératif. La route {@code /count} (route fonctionnelle
 * côté réactif) est ici un simple {@code @GetMapping} — même contrat HTTP.
 */
@RestController
@RequestMapping("/api/alerts")
public class AlertController {

    private final AlertService service;

    public AlertController(AlertService service) {
        this.service = service;
    }

    @GetMapping
    public List<Alert> list() {
        return service.all();
    }

    @GetMapping("/{id}")
    public ResponseEntity<Alert> byId(@PathVariable String id) {
        return service.byId(id)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Alert create(@Valid @RequestBody AlertRule rule) {
        return service.create(rule);
    }

    @GetMapping("/count")
    public Map<String, Long> count() {
        return Map.of("count", service.count());
    }
}
