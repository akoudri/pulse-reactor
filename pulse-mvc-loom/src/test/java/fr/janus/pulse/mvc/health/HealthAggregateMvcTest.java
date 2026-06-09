package fr.janus.pulse.mvc.health;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.hamcrest.Matchers.is;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Fan-out bloquant {@code /api/health/aggregate} (RestClient + virtual threads) contre des
 * upstreams WireMock. Doit survivre au lent et à l'instable, comme le jumeau réactif.
 */
@SpringBootTest
class HealthAggregateMvcTest {

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
    private WebApplicationContext wac;

    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        wireMock.resetAll();
        mvc = MockMvcBuilders.webAppContextSetup(wac).build();
    }

    @Test
    @DisplayName("survit au lent (700ms<800ms) et à l'instable (503 puis 200 via retry) → overall UP")
    void aggregateHealthyDespiteSlowAndFlaky() throws Exception {
        wireMock.stubFor(WireMock.get("/health/fast").willReturn(WireMock.okJson("{\"status\":\"UP\"}")));
        wireMock.stubFor(WireMock.get("/health/slow").willReturn(WireMock.okJson("{\"status\":\"UP\"}").withFixedDelay(700)));
        wireMock.stubFor(WireMock.get("/health/unstable").inScenario("flaky")
                .whenScenarioStateIs(STARTED)
                .willReturn(WireMock.serviceUnavailable())
                .willSetStateTo("recovered"));
        wireMock.stubFor(WireMock.get("/health/unstable").inScenario("flaky")
                .whenScenarioStateIs("recovered")
                .willReturn(WireMock.okJson("{\"status\":\"UP\"}")));

        mvc.perform(get("/api/health/aggregate"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.overall", is("UP")))
                .andExpect(jsonPath("$.upstreams.length()", is(3)));
    }

    @Test
    @DisplayName("instable durablement KO → repli DOWN, overall DEGRADED")
    void aggregateDegradedWhenOneStaysDown() throws Exception {
        wireMock.stubFor(WireMock.get("/health/fast").willReturn(WireMock.okJson("{\"status\":\"UP\"}")));
        wireMock.stubFor(WireMock.get("/health/slow").willReturn(WireMock.okJson("{\"status\":\"UP\"}")));
        wireMock.stubFor(WireMock.get("/health/unstable").willReturn(WireMock.serviceUnavailable()));

        mvc.perform(get("/api/health/aggregate"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.overall", is("DEGRADED")))
                // combine() trie par nom : [fast, slow, unstable] → unstable en index 2.
                .andExpect(jsonPath("$.upstreams[2].name", is("unstable")))
                .andExpect(jsonPath("$.upstreams[2].status", is("DOWN")));
    }
}
