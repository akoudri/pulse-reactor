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
        // TODO: save() insère et renvoie l'entité avec son id généré ; findById() la relit
        //       — piloter via StepVerifier (jamais de block())
    }

    @Test
    @DisplayName("findByMetricName() (query method dérivée) ne renvoie que la bonne métrique")
    void findByMetricNameFiltersByMetric() {
        // TODO: vérifier que la query method dérivée findByMetricName ne renvoie que la
        //       métrique demandée (StepVerifier)
    }
}
