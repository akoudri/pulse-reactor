package fr.janus.pulse.mvc.alert;

import com.jayway.jsonpath.JsonPath;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import static org.hamcrest.Matchers.is;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Contrat HTTP du jumeau via {@link MockMvc} (lié au contexte via {@code webAppContextSetup}
 * pour ne pas dépendre d'un module d'auto-config de test). Doit refléter le jumeau réactif.
 *
 * <p>On construit le corps JSON à la main et on lit la réponse via JsonPath : pas de
 * dépendance directe à Jackson (Boot 4 = Jackson 3, package {@code tools.jackson}).
 */
@SpringBootTest
class AlertApiMvcTest {

    @Autowired
    private WebApplicationContext wac;

    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        mvc = MockMvcBuilders.webAppContextSetup(wac).build();
    }

    @Test
    @DisplayName("POST crée (201) puis GET /{id} la retrouve")
    void createThenFetch() throws Exception {
        // TODO : POST crée (201) puis GET /{id} la retrouve.
    }

    @Test
    @DisplayName("GET /{id} inconnu → 404")
    void unknownIdReturns404() throws Exception {
        // TODO : GET /{id} inconnu → 404.
    }

    @Test
    @DisplayName("POST metricName vide → 400 (validation)")
    void invalidRuleReturns400() throws Exception {
        // TODO : POST metricName vide → 400 (validation).
    }

    @Test
    @DisplayName("GET /api/alerts/count → 200 avec un compteur")
    void countEndpoint() throws Exception {
        // TODO : GET /api/alerts/count → 200 avec un compteur.
    }
}
