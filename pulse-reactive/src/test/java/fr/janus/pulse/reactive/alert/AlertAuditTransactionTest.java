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
        // TODO: createWithAudit avec un metricName court → l'alerte ET sa ligne d'audit sont
        //       persistées dans la même transaction (vérifier via StepVerifier)
    }

    @Test
    @DisplayName("échec de l'audit → rollback : l'alerte n'est PAS persistée")
    void rollsBackAlertWhenAuditFails() {
        // TODO: forcer l'échec de l'audit (metricName trop long pour audit_log.target) et
        //       prouver le ROLLBACK : aucune alerte sur cette métrique n'est persistée
    }

    private reactor.core.publisher.Mono<Long> countAuditFor(String metric) {
        // TODO: compter les lignes d'audit liées à cette métrique via DatabaseClient
        return null;
    }
}
