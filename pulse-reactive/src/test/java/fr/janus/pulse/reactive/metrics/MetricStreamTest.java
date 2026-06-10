package fr.janus.pulse.reactive.metrics;

import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;

import fr.janus.pulse.common.EnrichedSample;
import fr.janus.pulse.common.MetricSample;
import fr.janus.pulse.reactive.AbstractPostgresIntegrationTest;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.web.reactive.function.client.ExchangeFilterFunctions.basicAuthentication;

/**
 * Vérifie le flux SSE {@code /api/metrics/stream} : le corps {@code text/event-stream} sert le
 * flux issu de Kafka (pont → {@code normalize/enrich}), décodé en {@link EnrichedSample} et
 * piloté via {@link StepVerifier}.
 *
 * <p>Vrai serveur ({@code RANDOM_PORT}) : WebFlux ne committe la réponse SSE qu'au premier
 * élément émis, donc il faut un flux qui produit réellement. Le simulateur d'agents Kafka
 * étant éteint en test (pas de broker ici), on alimente le pont {@link MetricStream}
 * <em>directement</em> via un driver d'intervalle local — déterministe et découplé de Kafka.
 * Les échantillons traversent alors le vrai pipeline d'ingestion avant d'être poussés en SSE.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class MetricStreamTest extends AbstractPostgresIntegrationTest {

    @Value("${local.server.port}")
    private int port;

    @Autowired
    private MetricStream metricStream;

    private WebTestClient client;
    private Disposable feeder;

    @BeforeEach
    void setUp() {
        client = WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                // /api/metrics/stream est sécurisé (lab J4-2 A) → auth HTTP Basic.
                .filter(basicAuthentication("user", "password"))
                .build();
        // Pousse en continu dans le pont (source hot) : les abonnés SSE qui se connectent
        // reçoivent les émissions suivantes (multicast, sans rejeu).
        feeder = Flux.interval(Duration.ofMillis(100))
                .map(tick -> new MetricSample("agent-sim", "pulse.cpu.load", tick % 100, Instant.now()))
                .subscribe(metricStream::emit);
    }

    @AfterEach
    void tearDown() {
        if (feeder != null) {
            feeder.dispose();
        }
    }

    @Test
    @DisplayName("le stream émet du text/event-stream : échantillons poussés dans le pont, normalisés + enrichis")
    void streamsEnrichedSamples() {
        Flux<EnrichedSample> body = client.get().uri("/api/metrics/stream")
                .accept(MediaType.TEXT_EVENT_STREAM)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().contentTypeCompatibleWith(MediaType.TEXT_EVENT_STREAM)
                .returnResult(EnrichedSample.class)
                .getResponseBody();

        StepVerifier.create(body)
                .assertNext(es -> assertEquals("agent-sim", es.sample().agentId()))
                .assertNext(es -> {
                    assertEquals("pulse.cpu.load", es.sample().name());
                    assertEquals("eu-west-1", es.region(), "région enrichie via l'annuaire");
                })
                .thenCancel()
                .verify(Duration.ofSeconds(6));
    }
}
