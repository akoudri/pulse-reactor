package fr.janus.pulse.reactive.alert;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.r2dbc.core.DatabaseClient;

import fr.janus.pulse.common.AlertRule;
import fr.janus.pulse.common.Severity;
import fr.janus.pulse.reactive.AbstractPostgresIntegrationTest;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

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

    /**
     * Illustration de {@link TestPublisher} (reactor-test). Les deux tests ci-dessus prouvent le
     * rollback au niveau de la <em>base</em> : une vraie contrainte ({@code target} VARCHAR étroit)
     * fait échouer l'audit. Ici on descend au niveau des <strong>signaux réactifs</strong> : on
     * reconstitue la composition de {@link AlertService#createWithAudit} — « sauver l'alerte, PUIS
     * écrire l'audit, puis rendre l'alerte » — mais on remplace la jambe d'audit par un
     * {@code TestPublisher<Void>} que l'on <strong>pilote à la main</strong>.
     *
     * <p>Ce que {@code TestPublisher} apporte (vs un simple {@code Mono.error(...)}) : on maîtrise
     * l'<em>instant</em> des signaux — l'erreur est émise via {@code then()}, donc <strong>après</strong>
     * la souscription, pas à l'assemblage — et l'on peut ensuite <em>asserter sur la source elle-même</em>
     * ({@link TestPublisher#assertWasSubscribed()}, {@link TestPublisher#assertWasRequested()}). On prouve
     * ainsi, sans base ni transaction, que l'échec de l'audit <strong>court-circuite</strong> l'émission
     * de l'alerte : l'analogue, au niveau du flux, du rollback prouvé plus haut.
     */
    @Test
    @DisplayName("TestPublisher : l'échec de la jambe d'audit court-circuite l'alerte (niveau signal)")
    void auditFailureShortCircuitsAlertWithTestPublisher() {
        // Source pilotable et CONFORME (demande respectée) : un Publisher<Void> dont le test
        // déclenche lui-même next/complete/error — ici une erreur, au moment de son choix.
        TestPublisher<Void> audit = TestPublisher.create();

        // Même forme que createWithAudit : alerte « sauvée », PUIS audit, PUIS on rend l'alerte.
        Mono<String> savedThenAudited =
                Mono.just("alert-42")
                        .flatMap(saved -> Mono.from(audit).thenReturn(saved));

        StepVerifier.create(savedThenAudited)
                // L'audit échoue APRÈS la souscription : c'est le test qui choisit l'instant exact.
                .then(() -> audit.error(new IllegalStateException("écriture d'audit refusée")))
                // L'erreur de l'audit remonte : l'alerte (« alert-42 ») n'est jamais émise.
                .expectErrorMessage("écriture d'audit refusée")
                .verify();

        // TestPublisher s'auto-vérifie : la jambe d'audit a bien été atteinte (souscrite et demandée),
        // ce qui confirme l'ordre de composition « alerte -> audit » (l'alerte n'a pas court-circuité l'audit).
        audit.assertWasSubscribed();
        audit.assertWasRequested();
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
