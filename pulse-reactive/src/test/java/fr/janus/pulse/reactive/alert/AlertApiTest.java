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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
class AlertApiTest {

    @Autowired
    private ApplicationContext context;

    private WebTestClient client;

    @BeforeEach
    void setUp() {
        client = WebTestClient.bindToApplicationContext(context).build();
    }

    @Test
    @DisplayName("POST crée une alerte (201), GET /{id} la retrouve, GET liste la contient")
    void createThenFetch() {
        // TODO : POST crée une alerte (201), GET /{id} la retrouve, GET liste la contient.
    }

    @Test
    @DisplayName("GET /{id} inconnu → 404")
    void unknownIdReturns404() {
        // TODO : GET /{id} inconnu → 404.
    }

    @Test
    @DisplayName("POST avec metricName vide → 400 (validation)")
    void invalidRuleReturns400() {
        // TODO : POST avec metricName vide → 400 (validation).
    }

    @Test
    @DisplayName("GET /api/alerts/count (route fonctionnelle) → 200 avec un compteur")
    void functionalCountRoute() {
        // TODO : GET /api/alerts/count (route fonctionnelle) → 200 avec un compteur.
    }
}
