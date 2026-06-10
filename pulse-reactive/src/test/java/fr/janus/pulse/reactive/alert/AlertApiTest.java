package fr.janus.pulse.reactive.alert;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;

import fr.janus.pulse.common.Alert;
import fr.janus.pulse.common.AlertRule;
import fr.janus.pulse.common.Severity;
import fr.janus.pulse.reactive.AbstractPostgresIntegrationTest;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.web.reactive.function.client.ExchangeFilterFunctions.basicAuthentication;

/**
 * Vérifie le contrat HTTP de l'API d'alertes via {@link WebTestClient} lié au contexte
 * applicatif (serveur mock). Couvre le contrôleur annoté et la route fonctionnelle.
 *
 * <p>On lie {@code WebTestClient} via {@code bindToApplicationContext} plutôt que
 * {@code @AutoConfigureWebTestClient} : en Boot 4 ce module d'auto-config de test n'est pas
 * transitif depuis {@code spring-boot-starter-test}, et ce binding ne dépend que de
 * spring-test (déjà présent via WebFlux).
 */
@SpringBootTest
class AlertApiTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private ApplicationContext context;

    private WebTestClient client;

    @BeforeEach
    void setUp() {
        // L'API est désormais sécurisée (lab J4-2 A) : on authentifie le client en HTTP Basic
        // (utilisateur en mémoire défini dans SecurityConfig). La preuve d'association au
        // principal et l'isolation entre utilisateurs sont couvertes par AlertSecurityTest.
        client = WebTestClient.bindToApplicationContext(context)
                .configureClient()
                .filter(basicAuthentication("user", "password"))
                .build();
    }

    @Test
    @DisplayName("POST crée une alerte (201), GET /{id} la retrouve, GET liste la contient")
    void createThenFetch() {
        AlertRule rule = new AlertRule("pulse.cpu.load", 90.0, Severity.CRITICAL);

        Alert created = client.post().uri("/api/alerts")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(rule)
                .exchange()
                .expectStatus().isCreated()
                .expectBody(Alert.class)
                .returnResult()
                .getResponseBody();

        assertNotNull(created, "le POST doit renvoyer l'alerte créée");

        client.get().uri("/api/alerts/{id}", created.id())
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.metricName").isEqualTo("pulse.cpu.load")
                .jsonPath("$.severity").isEqualTo("CRITICAL");

        client.get().uri("/api/alerts")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(Alert.class)
                .value(list -> assertTrue(
                        list.stream().anyMatch(a -> a.id().equals(created.id())),
                        "l'alerte créée doit figurer dans la liste"));
    }

    @Test
    @DisplayName("GET /{id} inconnu → 404")
    void unknownIdReturns404() {
        client.get().uri("/api/alerts/{id}", "does-not-exist")
                .exchange()
                .expectStatus().isNotFound();
    }

    @Test
    @DisplayName("POST avec metricName vide → 400 (validation)")
    void invalidRuleReturns400() {
        AlertRule invalid = new AlertRule("", 1.0, Severity.INFO);

        client.post().uri("/api/alerts")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(invalid)
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    @DisplayName("GET /api/alerts/count (route fonctionnelle) → 200 avec un compteur")
    void functionalCountRoute() {
        // On crée une alerte pour garantir count >= 1, sans dépendre de l'état global.
        client.post().uri("/api/alerts")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(new AlertRule("pulse.mem.used", 80.0, Severity.WARNING))
                .exchange()
                .expectStatus().isCreated();

        client.get().uri("/api/alerts/count")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.count").isNumber();
    }
}
