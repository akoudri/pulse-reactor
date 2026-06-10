package fr.janus.pulse.reactive.alert;

import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import fr.janus.pulse.common.Alert;
import fr.janus.pulse.common.AlertRule;
import fr.janus.pulse.reactive.context.TraceContextFilter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Service des alertes, désormais adossé à <strong>PostgreSQL via R2DBC</strong> (le stockage
 * en mémoire du lab j2-2 a disparu). Les méthodes restent purement réactives : on compose
 * des {@code Mono}/{@code Flux} et on mappe l'entité de persistance vers le DTO d'API à la
 * frontière — aucun {@code block()}.
 */
@Service
public class AlertService {

    private static final Logger log = LoggerFactory.getLogger(AlertService.class);

    private final AlertRepository repository;
    private final DatabaseClient databaseClient;

    public AlertService(AlertRepository repository, DatabaseClient databaseClient) {
        this.repository = repository;
        this.databaseClient = databaseClient;
    }

    public Flux<Alert> all() {
        // TODO: renvoyer toutes les alertes du repository, mappées en DTO
        return null;
    }

    public Mono<Alert> byId(String id) {
        // TODO: convertir l'id String en clé Long (Mono.empty() si non numérique → 404)
        // TODO: via Mono.deferContextual, logguer avant l'accès R2DBC en lisant le tenant du
        //       Context, puis findById ; mapper en DTO et logguer après l'accès
        return null;
    }

    public Mono<Alert> create(AlertRule rule) {
        // TODO: sauvegarder une nouvelle AlertEntity issue de la règle, puis mapper en DTO
        return null;
    }

    public Mono<Long> count() {
        // TODO: déléguer le comptage au repository
        return null;
    }

    /** Alertes d'une métrique donnée (s'appuie sur la query method dérivée). */
    public Flux<Alert> byMetric(String metricName) {
        // TODO: query method dérivée findByMetricName, mappée en DTO
        return null;
    }

    /**
     * Crée une alerte ET sa ligne d'audit dans la <strong>même transaction réactive</strong>.
     * {@code @Transactional} sur une méthode qui renvoie un {@code Mono} s'appuie sur le
     * {@code R2dbcTransactionManager} (auto-configuré) et le {@code Context} Reactor : si
     * l'écriture d'audit échoue, l'insert de l'alerte est <strong>rollback</strong> — rien
     * n'est persisté. C'est ce que prouve le test dédié.
     */
    @Transactional
    public Mono<Alert> createWithAudit(AlertRule rule) {
        // TODO: sauvegarder l'alerte PUIS écrire l'audit dans la même transaction, mapper en DTO
        return null;
    }

    /**
     * Insère la trace d'audit via {@link DatabaseClient}. La colonne {@code target} est
     * étroite (VARCHAR(20)) : un {@code metricName} plus long fait échouer cet insert, ce qui
     * déclenche le rollback de toute la transaction (alerte comprise) — le levier du test.
     */
    private Mono<Void> writeAudit(AlertEntity saved) {
        // TODO: insérer une ligne dans audit_log via DatabaseClient (l'échec déclenche le rollback)
        return null;
    }

    /**
     * Agrégat « nombre d'alertes par métrique » via {@link DatabaseClient} : un {@code GROUP BY}
     * que la query method ne sait pas exprimer. Tout reste réactif — la requête renvoie un
     * {@code Flux}, rien ne s'exécute avant la souscription.
     */
    public Flux<MetricAlertCount> countByMetric() {
        // TODO: agrégat GROUP BY metric_name via DatabaseClient, renvoyé en Flux<MetricAlertCount>
        return null;
    }

    /** Mapping entité R2DBC → DTO d'API (id Long → String pour respecter le contrat). */
    private static Alert toDto(AlertEntity entity) {
        // TODO: mapper l'entité R2DBC vers le DTO Alert (id Long → String)
        return null;
    }
}
