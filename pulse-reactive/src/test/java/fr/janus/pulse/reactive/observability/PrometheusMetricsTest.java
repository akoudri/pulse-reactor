package fr.janus.pulse.reactive.observability;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.reactive.server.WebTestClient;

import fr.janus.pulse.reactive.AbstractPostgresIntegrationTest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.web.reactive.function.client.ExchangeFilterFunctions.basicAuthentication;

/**
 * Vérifie que {@code /actuator/prometheus} expose les métriques d'ingestion, d'agrégation et
 * d'ouverture de circuit (lab J3-2 D). Vrai serveur ({@code RANDOM_PORT}) pour servir l'endpoint
 * actuator.
 *
 * <p>Les compteurs/timer sont enregistrés tôt (constructeurs de beans) : ils apparaissent dès le
 * démarrage, à zéro. On déclenche d'abord une agrégation pour alimenter le {@code Timer}.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class PrometheusMetricsTest extends AbstractPostgresIntegrationTest {

    @Value("${local.server.port}")
    private int port;

    private WebTestClient client;

    @BeforeEach
    void setUp() {
        // /api/health/aggregate est sécurisé (lab J4-2 A) → on authentifie ; /actuator/prometheus
        // reste ouvert (permitAll), l'en-tête Basic est alors simplement ignoré.
        client = WebTestClient.bindToServer().baseUrl("http://localhost:" + port)
                .filter(basicAuthentication("user", "password"))
                .build();
    }

    @Test
    @DisplayName("/actuator/prometheus expose ingestion / agrégation / ouverture de circuit")
    void exposesPulseMetrics() {
        // Déclenche une agrégation : le Timer enregistre au moins une observation. Les upstreams
        // ne sont pas stubbés → repli DOWN, mais l'endpoint répond 200 et le Timer est alimenté.
        client.get().uri("/api/health/aggregate").exchange().expectStatus().isOk();

        String body = client.get().uri("/actuator/prometheus")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertThat(body)
                .as("métriques Pulse exposées au format Prometheus")
                .contains("pulse_ingestion_samples_total")   // Counter d'ingestion
                .contains("pulse_health_aggregate")          // Timer d'agrégation
                .contains("pulse_circuit_open_total");       // Counter d'ouvertures de circuit
    }
}
