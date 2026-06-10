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
        return repository.findAll().map(AlertService::toDto);
    }

    public Mono<Alert> byId(String id) {
        // L'API expose un id String ; la clé technique est un Long. Un id non numérique
        // ne correspond à rien en base → Mono vide (→ 404 côté contrôleur).
        Long key;
        try {
            key = Long.valueOf(id);
        } catch (NumberFormatException notNumeric) {
            return Mono.empty();
        }
        // deferContextual : lecture EXPLICITE du Context au fond du pipeline (le tenant),
        // à côté de l'accès R2DBC. Le traceId, lui, est dans le MDC via la propagation
        // automatique — il apparaît donc dans ce log comme dans celui du contrôleur, malgré
        // le changement de thread entre l'entrée HTTP et le driver R2DBC.
        return Mono.deferContextual(ctx -> {
                    log.info("avant accès R2DBC findById({}) — tenant={}",
                            key, ctx.getOrDefault(TraceContextFilter.TENANT_KEY, "unknown"));
                    return repository.findById(key);
                })
                .map(AlertService::toDto)
                .doOnTerminate(() -> log.info("après accès R2DBC findById({})", key));
    }

    public Mono<Alert> create(AlertRule rule) {
        return repository.save(AlertEntity.newAlert(
                        rule.metricName(), rule.threshold(), rule.severity(), Instant.now()))
                .map(AlertService::toDto);
    }

    public Mono<Long> count() {
        return repository.count();
    }

    /** Alertes d'une métrique donnée (s'appuie sur la query method dérivée). */
    public Flux<Alert> byMetric(String metricName) {
        return repository.findByMetricName(metricName).map(AlertService::toDto);
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
        return repository.save(AlertEntity.newAlert(
                        rule.metricName(), rule.threshold(), rule.severity(), Instant.now()))
                .flatMap(saved -> writeAudit(saved).thenReturn(saved))
                .map(AlertService::toDto);
    }

    /**
     * Insère la trace d'audit via {@link DatabaseClient}. La colonne {@code target} est
     * étroite (VARCHAR(20)) : un {@code metricName} plus long fait échouer cet insert, ce qui
     * déclenche le rollback de toute la transaction (alerte comprise) — le levier du test.
     */
    private Mono<Void> writeAudit(AlertEntity saved) {
        return databaseClient.sql("""
                        INSERT INTO audit_log(alert_id, action, target, logged_at)
                        VALUES (:alertId, :action, :target, :loggedAt)
                        """)
                .bind("alertId", saved.id())
                .bind("action", "CREATE")
                .bind("target", saved.metricName())
                .bind("loggedAt", Instant.now())
                .fetch().rowsUpdated()
                .then();
    }

    /**
     * Agrégat « nombre d'alertes par métrique » via {@link DatabaseClient} : un {@code GROUP BY}
     * que la query method ne sait pas exprimer. Tout reste réactif — la requête renvoie un
     * {@code Flux}, rien ne s'exécute avant la souscription.
     */
    public Flux<MetricAlertCount> countByMetric() {
        return databaseClient.sql("""
                        SELECT metric_name, COUNT(*) AS alert_count
                        FROM alert
                        GROUP BY metric_name
                        ORDER BY metric_name
                        """)
                .map((row, metadata) -> new MetricAlertCount(
                        row.get("metric_name", String.class),
                        row.get("alert_count", Long.class)))
                .all();
    }

    /** Mapping entité R2DBC → DTO d'API (id Long → String pour respecter le contrat). */
    private static Alert toDto(AlertEntity entity) {
        return new Alert(
                String.valueOf(entity.id()),
                entity.metricName(),
                entity.threshold(),
                entity.severity(),
                entity.createdAt());
    }
}
