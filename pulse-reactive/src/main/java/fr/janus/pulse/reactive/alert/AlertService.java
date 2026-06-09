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
        return Flux.fromIterable(store.values());
    }

    public Mono<Alert> byId(String id) {
        return Mono.justOrEmpty(store.get(id));
    }

    public Mono<Alert> create(AlertRule rule) {
        // fromSupplier : la génération d'id et l'écriture ne s'exécutent qu'à la souscription.
        return Mono.fromSupplier(() -> {
            String id = UUID.randomUUID().toString();
            Alert alert = Alert.from(id, rule, Instant.now());
            store.put(id, alert);
            return alert;
        });
    }

    public Mono<Long> count() {
        return Mono.fromSupplier(() -> (long) store.size());
    }
}
