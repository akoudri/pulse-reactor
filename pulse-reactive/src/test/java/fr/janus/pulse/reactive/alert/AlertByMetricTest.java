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

import static org.springframework.web.reactive.function.client.ExchangeFilterFunctions.basicAuthentication;

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
        client = WebTestClient.bindToApplicationContext(context)
                .configureClient()
                .filter(basicAuthentication("user", "password"))
                .build();
    }

    @Test
    @DisplayName("by-metric agrège le nombre d'alertes par métrique (GROUP BY)")
    void aggregatesCountPerMetric() {
        String metric = "pulse.gc.pause." + System.nanoTime(); // unique → comptage déterministe
        createAlert(metric, Severity.WARNING);
        createAlert(metric, Severity.CRITICAL);

        client.get().uri("/api/alerts/by-metric")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(MetricAlertCount.class)
                .value(rows -> {
                    long forMetric = rows.stream()
                            .filter(r -> r.metricName().equals(metric))
                            .mapToLong(MetricAlertCount::count)
                            .findFirst()
                            .orElse(-1);
                    org.junit.jupiter.api.Assertions.assertEquals(2L, forMetric,
                            "deux alertes ont été créées sur cette métrique");
                });
    }

    private void createAlert(String metric, Severity severity) {
        client.post().uri("/api/alerts")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(new AlertRule(metric, 50.0, severity))
                .exchange()
                .expectStatus().isCreated();
    }
}
