package fr.janus.pulse.mvc.alert;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Service;

import fr.janus.pulse.common.Alert;
import fr.janus.pulse.common.AlertRule;

/**
 * Stockage des alertes en mémoire, en style <strong>impératif bloquant</strong> :
 * renvoie directement {@link List}/{@link Alert} (pas de Publisher). Jumeau du
 * {@code AlertService} réactif, même comportement fonctionnel.
 */
@Service
public class AlertService {

    private final Map<String, Alert> store = new ConcurrentHashMap<>();

    public List<Alert> all() {
        return List.copyOf(store.values());
    }

    public Optional<Alert> byId(String id) {
        return Optional.ofNullable(store.get(id));
    }

    public Alert create(AlertRule rule) {
        String id = UUID.randomUUID().toString();
        Alert alert = Alert.from(id, rule, Instant.now());
        store.put(id, alert);
        return alert;
    }

    public long count() {
        return store.size();
    }
}
