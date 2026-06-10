package fr.janus.pulse.reactive.alert;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;

import fr.janus.pulse.common.AlertRule;
import fr.janus.pulse.common.Severity;
import fr.janus.pulse.reactive.AbstractPostgresIntegrationTest;

/**
 * Vérifie l'agrégat {@code GET /api/alerts/by-metric} (DatabaseClient, GROUP BY) de bout en
 * bout via {@link WebTestClient} contre une PostgreSQL Testcontainers.
 */
@SpringBootTest
class AlertByMetricTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private ApplicationContext context;

    private WebTestClient client;

    @BeforeEach
    void setUp() {
        client = WebTestClient.bindToApplicationContext(context).build();
    }

    @Test
    @DisplayName("by-metric agrège le nombre d'alertes par métrique (GROUP BY)")
    void aggregatesCountPerMetric() {
        // TODO: créer 2 alertes sur une même métrique, appeler GET /api/alerts/by-metric
        //       et vérifier que le count agrégé (GROUP BY) vaut bien 2 pour cette métrique
    }

    private void createAlert(String metric, Severity severity) {
        // TODO: POST /api/alerts pour créer une alerte (attendre 201 Created)
    }
}
