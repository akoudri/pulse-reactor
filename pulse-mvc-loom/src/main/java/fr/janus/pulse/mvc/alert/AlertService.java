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
        // TODO : retourner toutes les alertes du store.
    }

    public Optional<Alert> byId(String id) {
        // TODO : retourner l'alerte correspondant à l'id (Optional vide si absente).
    }

    public Alert create(AlertRule rule) {
        // TODO : créer une alerte à partir de la règle et la stocker.
    }

    public long count() {
        // TODO : retourner le nombre d'alertes du store.
    }
}
