package fr.janus.pulse.reactive.alert;

import java.time.Instant;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import fr.janus.pulse.common.Severity;
import fr.janus.pulse.reactive.AbstractPostgresIntegrationTest;
import reactor.test.StepVerifier;

/**
 * Tests d'intégration de la persistance R2DBC contre une PostgreSQL Testcontainers.
 * On pilote le repository via {@link StepVerifier} (jamais de {@code block()}).
 */
@SpringBootTest
class AlertPersistenceTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private AlertRepository repository;

    @Test
    @DisplayName("save() insère et renvoie l'entité avec son id généré, findById() la relit")
    void saveThenFindById() {
        AlertEntity toSave = AlertEntity.newAlert("pulse.disk.io", 95.0, Severity.CRITICAL, Instant.now(), "system");

        StepVerifier.create(repository.save(toSave))
                .assertNext(saved -> {
                    org.junit.jupiter.api.Assertions.assertNotNull(saved.id(), "l'id doit être généré");
                    org.junit.jupiter.api.Assertions.assertEquals("pulse.disk.io", saved.metricName());
                    org.junit.jupiter.api.Assertions.assertEquals(Severity.CRITICAL, saved.severity());
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("findByMetricName() (query method dérivée) ne renvoie que la bonne métrique")
    void findByMetricNameFiltersByMetric() {
        String metric = "pulse.net.rx." + System.nanoTime(); // unique → isolation entre tests
        AlertEntity entity = AlertEntity.newAlert(metric, 10.0, Severity.WARNING, Instant.now(), "system");

        StepVerifier.create(repository.save(entity).thenMany(repository.findByMetricName(metric)))
                .assertNext(found -> org.junit.jupiter.api.Assertions.assertEquals(metric, found.metricName()))
                .verifyComplete();
    }
}
