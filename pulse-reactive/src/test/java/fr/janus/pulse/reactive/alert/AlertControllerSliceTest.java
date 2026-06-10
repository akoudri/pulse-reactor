package fr.janus.pulse.reactive.alert;

import java.time.Instant;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;

import fr.janus.pulse.common.Alert;
import fr.janus.pulse.common.AlertRule;
import fr.janus.pulse.common.Severity;
import fr.janus.pulse.reactive.security.SecurityConfig;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.web.reactive.function.client.ExchangeFilterFunctions.basicAuthentication;

/**
 * Slice test du contrôleur d'alertes : {@link WebFluxTest} ne charge <strong>que</strong>
 * {@link AlertController} et l'infrastructure WebFlux (routing, codecs, validation), pas le
 * contexte applicatif complet — donc ni R2DBC, ni Kafka, ni Testcontainers. Le collaborateur
 * {@link AlertService} est remplacé par un mock via {@code @MockitoBean} (l'API de remplacement
 * de bean de spring-test ; {@code @MockBean} est déprécié). {@code WebTestClient} est
 * auto-configuré par {@code @WebFluxTest}.
 *
 * <p>On vérifie ici le <em>contrat HTTP</em> du contrôleur (codes, mapping, routage) isolément :
 * statut 200/201/404, sérialisation JSON, et 400 sur payload invalide (validation
 * {@code @Valid}). La logique de persistance est couverte ailleurs (tests d'intégration R2DBC).
 */
// La sécurité réactive (lab J4-2 A) s'applique aussi au slice : on importe SecurityConfig
// (utilisateurs en mémoire, non bloquant) et on authentifie le client en HTTP Basic. Le slice
// reste centré sur le contrat HTTP du contrôleur ; l'association au principal est testée ailleurs.
@WebFluxTest(AlertController.class)
@Import(SecurityConfig.class)
class AlertControllerSliceTest {

    private static final Instant AT = Instant.parse("2026-06-01T10:00:00Z");

    @Autowired
    private WebTestClient client;

    @MockitoBean
    private AlertService service;

    @BeforeEach
    void authenticate() {
        client = client.mutate().filter(basicAuthentication("user", "password")).build();
    }

    @Test
    @DisplayName("GET /api/alerts : la liste du service est sérialisée (200)")
    void listReturnsAlerts() {
        when(service.all()).thenReturn(Flux.just(
                new Alert("1", "pulse.cpu.load", 90.0, Severity.CRITICAL, AT),
                new Alert("2", "pulse.mem.used", 80.0, Severity.WARNING, AT)));

        client.get().uri("/api/alerts")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.length()").isEqualTo(2)
                .jsonPath("$[0].id").isEqualTo("1")
                .jsonPath("$[1].metricName").isEqualTo("pulse.mem.used");
    }

    @Test
    @DisplayName("GET /api/alerts/{id} présent : 200 + corps")
    void byIdFound() {
        when(service.byId("1")).thenReturn(
                Mono.just(new Alert("1", "pulse.cpu.load", 90.0, Severity.CRITICAL, AT)));

        client.get().uri("/api/alerts/1")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.id").isEqualTo("1")
                .jsonPath("$.severity").isEqualTo("CRITICAL");
    }

    @Test
    @DisplayName("GET /api/alerts/{id} absent : le Mono vide devient 404")
    void byIdNotFound() {
        when(service.byId("999")).thenReturn(Mono.empty());

        client.get().uri("/api/alerts/999")
                .exchange()
                .expectStatus().isNotFound();
    }

    @Test
    @DisplayName("POST /api/alerts valide : 201 + alerte créée")
    void createReturns201() {
        Alert created = new Alert("42", "pulse.cpu.load", 90.0, Severity.CRITICAL, AT);
        when(service.create(any(AlertRule.class))).thenReturn(Mono.just(created));

        client.post().uri("/api/alerts")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(new AlertRule("pulse.cpu.load", 90.0, Severity.CRITICAL))
                .exchange()
                .expectStatus().isCreated()
                .expectBody()
                .jsonPath("$.id").isEqualTo("42");
    }

    @Test
    @DisplayName("POST /api/alerts invalide (metricName vide) : la validation @Valid renvoie 400")
    void createInvalidReturns400() {
        // metricName vide viole @NotBlank → 400, sans même atteindre le service.
        client.post().uri("/api/alerts")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"metricName\":\"\",\"threshold\":90.0,\"severity\":\"CRITICAL\"}")
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    @DisplayName("GET /api/alerts/by-metric : l'agrégat GROUP BY est sérialisé (200)")
    void byMetricReturnsCounts() {
        when(service.countByMetric()).thenReturn(Flux.just(
                new MetricAlertCount("pulse.cpu.load", 3L),
                new MetricAlertCount("pulse.mem.used", 1L)));

        client.get().uri("/api/alerts/by-metric")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.length()").isEqualTo(2)
                .jsonPath("$[0].metricName").isEqualTo("pulse.cpu.load")
                .jsonPath("$[0].count").isEqualTo(3);
    }
}
