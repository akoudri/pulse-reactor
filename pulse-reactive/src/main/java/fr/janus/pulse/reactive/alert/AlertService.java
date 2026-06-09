package fr.janus.pulse.reactive.alert;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Service;

import fr.janus.pulse.common.Alert;
import fr.janus.pulse.common.AlertRule;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Stockage des alertes <strong>en mémoire</strong> (Map concurrente) — pas de base ici,
 * la persistance R2DBC arrive en J3. Les méthodes retournent des Publishers ; aucun
 * {@code block()}, le travail (création d'id, écriture) est différé dans le pipeline.
 */
@Service
public class AlertService {

    private final Map<String, Alert> store = new ConcurrentHashMap<>();

    public Flux<Alert> all() {
        // TODO : retourner toutes les alertes du store sous forme de Flux.
    }

    public Mono<Alert> byId(String id) {
        // TODO : retourner l'alerte correspondant à l'id (Mono vide si absente).
    }

    public Mono<Alert> create(AlertRule rule) {
        // TODO : créer une alerte à partir de la règle et la stocker, sans block().
    }

    public Mono<Long> count() {
        // TODO : retourner le nombre d'alertes du store.
    }
}
