package fr.janus.pulse.reactive.alert;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;

import fr.janus.pulse.common.Alert;
import fr.janus.pulse.common.AlertRule;
import fr.janus.pulse.common.Severity;
import fr.janus.pulse.reactive.AbstractPostgresIntegrationTest;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.mockUser;
import static org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.springSecurity;

/**
 * Sécurité réactive de bout en bout (lab J4-2 A). On prouve trois choses :
 * <ol>
 *   <li>un appel <strong>non authentifié</strong> sur {@code /api/**} est rejeté (401) ;</li>
 *   <li>l'alerte créée est <strong>associée au principal</strong> : l'utilisateur lu via
 *       {@code ReactiveSecurityContextHolder} <em>au fond du pipeline</em> (changement de thread
 *       depuis l'event-loop Netty) est bien celui qui a émis la requête — donc le
 *       {@code SecurityContext} a traversé le pipeline réactif (lien J3-1) ;</li>
 *   <li>l'<strong>isolation</strong> : un autre utilisateur ne voit pas les alertes du premier.</li>
 * </ol>
 *
 * <p>On utilise le support de test réactif de Spring Security : {@code springSecurity()} câble la
 * chaîne de sécurité dans le {@link WebTestClient} lié au contexte, et {@code mockUser(...)}
 * injecte un principal dans le {@code SecurityContext} réactif sans vrai login (le « utilisateur
 * mocké » du brief — équivalent réactif de {@code @WithMockUser}).
 */
@SpringBootTest
class AlertSecurityTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private ApplicationContext context;

    private WebTestClient client;

    @BeforeEach
    void setUp() {
        client = WebTestClient.bindToApplicationContext(context)
                .apply(springSecurity())
                .build();
    }

    @Test
    @DisplayName("non authentifié → 401 sur /api/**")
    void unauthenticatedIsRejected() {
        client.get().uri("/api/alerts")
                .exchange()
                .expectStatus().isUnauthorized();
    }

    @Test
    @DisplayName("l'alerte est associée au principal (ReactiveSecurityContextHolder) et isolée entre utilisateurs")
    void alertIsAssociatedToPrincipalAndIsolated() {
        WebTestClient alice = client.mutateWith(mockUser("alice"));
        WebTestClient bob = client.mutateWith(mockUser("bob"));

        // Métrique unique → l'alerte est identifiable sans dépendre de l'état global de la base.
        String metric = "pulse.sec.owner." + System.nanoTime();
        Alert created = alice.post().uri("/api/alerts")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(new AlertRule(metric, 42.0, Severity.WARNING))
                .exchange()
                .expectStatus().isCreated()
                .expectBody(Alert.class)
                .returnResult()
                .getResponseBody();
        assertNotNull(created, "le POST authentifié doit créer l'alerte");

        // alice retrouve SON alerte dans /mine : la lecture du principal au fond du pipeline a
        // bien renvoyé « alice » à la création (created_by) ET à la requête /mine.
        alice.get().uri("/api/alerts/mine")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(Alert.class)
                .value(mine -> assertTrue(
                        mine.stream().anyMatch(a -> a.metricName().equals(metric)),
                        "alice doit voir l'alerte qu'elle a créée"));

        // bob ne voit PAS l'alerte d'alice : isolation par principal.
        bob.get().uri("/api/alerts/mine")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(Alert.class)
                .value(mine -> assertFalse(
                        mine.stream().anyMatch(a -> a.metricName().equals(metric)),
                        "bob ne doit pas voir l'alerte d'alice"));
    }
}
