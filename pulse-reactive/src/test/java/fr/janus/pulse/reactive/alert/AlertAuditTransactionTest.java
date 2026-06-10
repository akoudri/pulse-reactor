package fr.janus.pulse.reactive.alert;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.r2dbc.core.DatabaseClient;

import fr.janus.pulse.common.AlertRule;
import fr.janus.pulse.common.Severity;
import fr.janus.pulse.reactive.AbstractPostgresIntegrationTest;
import reactor.test.StepVerifier;

/**
 * Prouve la transaction réactive de {@link AlertService#createWithAudit} : succès commun
 * (alerte + audit persistés) et surtout <strong>rollback</strong> quand l'audit échoue —
 * l'alerte ne doit alors PAS être persistée. Aucun {@code block()} : tout via StepVerifier.
 */
@SpringBootTest
class AlertAuditTransactionTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private AlertService service;

    @Autowired
    private AlertRepository repository;

    @Autowired
    private DatabaseClient databaseClient;

    @Test
    @DisplayName("succès : alerte ET ligne d'audit persistées dans la même transaction")
    void commitsAlertAndAudit() {
        String metric = "ok." + System.nanoTime(); // court (<64) → audit OK

        StepVerifier.create(service.createWithAudit(new AlertRule(metric, 70.0, Severity.WARNING)))
                .assertNext(alert -> org.junit.jupiter.api.Assertions.assertEquals(metric, alert.metricName()))
                .verifyComplete();

        // L'alerte est bien là...
        StepVerifier.create(repository.findByMetricName(metric))
                .expectNextCount(1)
                .verifyComplete();
        // ...et exactement une ligne d'audit y correspond.
        StepVerifier.create(countAuditFor(metric))
                .expectNext(1L)
                .verifyComplete();
    }

    @Test
    @DisplayName("échec de l'audit → rollback : l'alerte n'est PAS persistée")
    void rollsBackAlertWhenAuditFails() {
        // metricName > 64 caractères : l'insert dans audit_log.target (VARCHAR(64)) échoue,
        // alors que la colonne alert.metric_name (VARCHAR(120)) l'aurait accepté.
        String tooLong = "x".repeat(70) + "." + System.nanoTime();
        org.junit.jupiter.api.Assertions.assertTrue(tooLong.length() > 64 && tooLong.length() <= 120);

        StepVerifier.create(service.createWithAudit(new AlertRule(tooLong, 70.0, Severity.CRITICAL)))
                .expectError()
                .verify();

        // Rollback prouvé : aucune alerte sur cette métrique n'a survécu.
        StepVerifier.create(repository.findByMetricName(tooLong))
                .verifyComplete();
    }

    private reactor.core.publisher.Mono<Long> countAuditFor(String metric) {
        return databaseClient.sql("""
                        SELECT COUNT(*) AS c FROM audit_log a
                        JOIN alert al ON al.id = a.alert_id
                        WHERE al.metric_name = :metric
                        """)
                .bind("metric", metric)
                .map((row, meta) -> row.get("c", Long.class))
                .one();
    }
}
