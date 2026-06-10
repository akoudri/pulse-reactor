package fr.janus.pulse.reactive.health;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

import fr.janus.pulse.reactive.AbstractPostgresIntegrationTest;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static org.springframework.web.reactive.function.client.ExchangeFilterFunctions.basicAuthentication;

/**
 * Détection du goulot introduit en lab J4-2 B par <strong>BlockHound</strong>.
 *
 * <p>On démarre le contexte avec {@code pulse.profiling.fanout-enrichment=blocking} : l'étape
 * d'enrichissement du fan-out exécute un {@code Thread.sleep} <em>sur le thread courant</em> —
 * l'event-loop Netty qui porte la réponse WebClient, marqué « non bloquant ». BlockHound, actif
 * sur toute la session de test (lab J4-1 D), <strong>intercepte</strong> cet appel : chaque probe
 * échoue donc en {@code BlockingOperationError}, rattrapé par le repli {@code onErrorResume} → tous
 * les upstreams basculent {@code DOWN}, et l'agrégat global devient {@code DOWN}.
 *
 * <p>C'est exactement le signal recherché : <em>sans</em> le goulot (mode {@code off}, cf.
 * {@link HealthAggregateTest}), ces mêmes upstreams stubbés en 200/UP donnent un agrégat
 * {@code UP}. Le basculement en {@code DOWN} prouve que le blocage non isolé est révélé — on ne
 * l'élargit pas dans l'allowlist, on le corrige (mode {@code offloaded}, cf.
 * {@code docs/profiling-j4-2.md}).
 */
@SpringBootTest
@TestPropertySource(properties = "pulse.profiling.fanout-enrichment=blocking")
class FanoutBottleneckBlockHoundTest extends AbstractPostgresIntegrationTest {

    private static WireMockServer wireMock;

    @BeforeAll
    static void startWireMock() {
        wireMock = new WireMockServer(WireMockConfiguration.options().dynamicPort());
        wireMock.start();
    }

    @AfterAll
    static void stopWireMock() {
        wireMock.stop();
    }

    @DynamicPropertySource
    static void upstreamProperties(DynamicPropertyRegistry registry) {
        registry.add("pulse.upstream.base-url", () -> "http://localhost:" + wireMock.port());
    }

    @Autowired
    private ApplicationContext context;

    @Autowired
    private CircuitBreakerRegistry circuitBreakers;

    private WebTestClient client;

    @BeforeEach
    void setUp() {
        wireMock.resetAll();
        circuitBreakers.getAllCircuitBreakers().forEach(CircuitBreaker::reset);
        client = WebTestClient.bindToApplicationContext(context)
                .configureClient()
                .filter(basicAuthentication("user", "password"))
                .build();
    }

    @Test
    @DisplayName("mode 'blocking' : BlockHound intercepte le Thread.sleep sur l'event-loop → repli DOWN sur tous les upstreams")
    void blockHoundRevealsTheUnisolatedBlockingCall() {
        // Tous les upstreams répondent 200/UP : sans goulot, l'agrégat serait UP. Ici le
        // Thread.sleep non isolé est intercepté par BlockHound → chaque probe tombe en repli DOWN.
        wireMock.stubFor(get("/health/fast").willReturn(okJson("{\"status\":\"UP\"}")));
        wireMock.stubFor(get("/health/slow").willReturn(okJson("{\"status\":\"UP\"}")));
        wireMock.stubFor(get("/health/unstable").willReturn(okJson("{\"status\":\"UP\"}")));

        client.get().uri("/api/health/aggregate")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                // Le goulot est révélé : malgré des upstreams sains, tout bascule DOWN.
                .jsonPath("$.overall").isEqualTo("DOWN")
                .jsonPath("$.upstreams[0].status").isEqualTo("DOWN")
                .jsonPath("$.upstreams[1].status").isEqualTo("DOWN")
                .jsonPath("$.upstreams[2].status").isEqualTo("DOWN");
    }
}
