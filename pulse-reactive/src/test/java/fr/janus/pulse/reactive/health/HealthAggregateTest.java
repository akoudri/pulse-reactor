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
import org.springframework.test.web.reactive.server.WebTestClient;

import fr.janus.pulse.reactive.AbstractPostgresIntegrationTest;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.serviceUnavailable;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Vérifie le fan-out {@code /api/health/aggregate} contre des upstreams stubbés par
 * WireMock (un rapide, un lent ~700 ms, un instable), au travers de la vraie stack
 * WebClient + timeout + retry + repli.
 */
@SpringBootTest
class HealthAggregateTest extends AbstractPostgresIntegrationTest {

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
        // Repart d'un état circuit propre : l'état est partagé (bean singleton) entre les tests.
        circuitBreakers.getAllCircuitBreakers().forEach(CircuitBreaker::reset);
        client = WebTestClient.bindToApplicationContext(context).build();
    }

    @Test
    @DisplayName("survit au lent (700ms<800ms) et à l'instable (503 puis 200 via retry) → overall UP")
    void aggregateHealthyDespiteSlowAndFlaky() {
        wireMock.stubFor(get("/health/fast").willReturn(okJson("{\"status\":\"UP\"}")));
        wireMock.stubFor(get("/health/slow").willReturn(okJson("{\"status\":\"UP\"}").withFixedDelay(700)));
        // Instable : 503 d'abord, puis 200 — le retryWhen doit récupérer.
        wireMock.stubFor(get("/health/unstable").inScenario("flaky")
                .whenScenarioStateIs(STARTED)
                .willReturn(serviceUnavailable())
                .willSetStateTo("recovered"));
        wireMock.stubFor(get("/health/unstable").inScenario("flaky")
                .whenScenarioStateIs("recovered")
                .willReturn(okJson("{\"status\":\"UP\"}")));

        client.get().uri("/api/health/aggregate")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.overall").isEqualTo("UP")
                .jsonPath("$.upstreams.length()").isEqualTo(3);
    }

    @Test
    @DisplayName("instable durablement KO → repli DOWN, overall DEGRADED")
    void aggregateDegradedWhenOneStaysDown() {
        wireMock.stubFor(get("/health/fast").willReturn(okJson("{\"status\":\"UP\"}")));
        wireMock.stubFor(get("/health/slow").willReturn(okJson("{\"status\":\"UP\"}")));
        wireMock.stubFor(get("/health/unstable").willReturn(serviceUnavailable())); // toujours 503

        client.get().uri("/api/health/aggregate")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.overall").isEqualTo("DEGRADED")
                // combine() trie par nom : [fast, slow, unstable] → unstable en index 2.
                .jsonPath("$.upstreams[2].name").isEqualTo("unstable")
                .jsonPath("$.upstreams[2].status").isEqualTo("DOWN");
    }

    @Test
    @DisplayName("upstream durablement KO → le circuit s'OUVRE et bascule en repli (court-circuit)")
    void opensCircuitUnderSustainedFailureThenFallsBack() {
        wireMock.stubFor(get("/health/fast").willReturn(okJson("{\"status\":\"UP\"}")));
        wireMock.stubFor(get("/health/slow").willReturn(okJson("{\"status\":\"UP\"}")));
        wireMock.stubFor(get("/health/unstable").willReturn(serviceUnavailable())); // toujours 503

        CircuitBreaker unstable = circuitBreakers.circuitBreaker("unstable");

        // Quelques agrégations suffisent à dépasser minimumNumberOfCalls (3) à 100% d'échec.
        for (int i = 0; i < 5; i++) {
            client.get().uri("/api/health/aggregate").exchange().expectStatus().isOk();
        }

        // Le circuit de l'upstream KO est désormais ouvert.
        assertEquals(CircuitBreaker.State.OPEN, unstable.getState(),
                "le circuit doit s'ouvrir sous échec répété");

        // Appel suivant : court-circuit (CallNotPermittedException) → repli DOWN immédiat,
        // sans requête réseau. fast/slow restent UP → overall DEGRADED.
        client.get().uri("/api/health/aggregate")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.overall").isEqualTo("DEGRADED")
                .jsonPath("$.upstreams[2].name").isEqualTo("unstable")
                .jsonPath("$.upstreams[2].status").isEqualTo("DOWN");
    }
}
