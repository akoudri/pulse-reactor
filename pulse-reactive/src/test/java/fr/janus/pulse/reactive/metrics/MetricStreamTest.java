package fr.janus.pulse.reactive.metrics;

import java.time.Duration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;

import fr.janus.pulse.common.MetricSample;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Vérifie le flux SSE {@code /api/metrics/stream} : le corps {@code text/event-stream} est
 * décodé en {@link MetricSample} et piloté via {@link StepVerifier} ({@code returnResult} +
 * assertions sur le corps). On laisse le simulateur d'agents pousser dans le sink.
 *
 * <p>Vrai serveur ({@code RANDOM_PORT}) : WebFlux ne committe la réponse SSE qu'au premier
 * élément émis, donc il faut un flux qui produit réellement — c'est le rôle du simulateur.
 * Port injecté via {@code ${local.server.port}}.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class MetricStreamTest {

    @Value("${local.server.port}")
    private int port;

    private WebTestClient client;

    @BeforeEach
    void setUp() {
        client = WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .build();
    }

    @Test
    @DisplayName("le stream émet du text/event-stream et délivre les échantillons du simulateur")
    void streamsSimulatedSamples() {
        Flux<MetricSample> body = client.get().uri("/api/metrics/stream")
                .accept(MediaType.TEXT_EVENT_STREAM)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().contentTypeCompatibleWith(MediaType.TEXT_EVENT_STREAM)
                .returnResult(MetricSample.class)
                .getResponseBody();

        StepVerifier.create(body)
                .assertNext(sample -> assertEquals("agent-sim", sample.agentId()))
                .assertNext(sample -> assertEquals("pulse.cpu.load", sample.name()))
                .thenCancel()
                .verify(Duration.ofSeconds(6));
    }
}
