package fr.janus.pulse.reactive.metrics;

import java.time.Instant;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;

import fr.janus.pulse.common.EnrichedSample;
import fr.janus.pulse.common.MetricSample;
import fr.janus.pulse.reactive.ingestion.MetricIngestion;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

/**
 * Slice test SSE de {@link MetricStreamController} : {@code @WebFluxTest} ne charge que le
 * contrôleur et l'infra WebFlux (codec {@code text/event-stream} compris), pas le pipeline
 * d'ingestion ni le broker. La source ({@link MetricIngestion#processed()}) est un
 * {@code @MockitoBean} qu'on alimente d'un flux déterministe — on isole donc le découpage
 * SSE de toute dépendance Kafka/R2DBC.
 *
 * <p>On récupère le corps via {@code returnResult(...).getResponseBody()} puis on le pilote
 * au {@link StepVerifier} avec {@code take(n)} : c'est le motif de test d'un flux SSE
 * potentiellement long, dont on ne consomme qu'un préfixe avant d'annuler proprement.
 *
 * <p>Source <em>finie</em> (trois échantillons) volontairement : le serveur mock de
 * {@code @WebFluxTest} ne committe la réponse SSE qu'au premier élément ; une source qui
 * produit réellement évite le blocage observé sur un flux qui n'émet jamais. {@code take(2)}
 * prélève un préfixe strict du flux.
 */
@WebFluxTest(MetricStreamController.class)
class MetricStreamSliceTest {

    private static final Instant AT = Instant.parse("2026-06-01T10:00:00Z");

    @Autowired
    private WebTestClient client;

    @MockitoBean
    private MetricIngestion ingestion;

    private static EnrichedSample enriched(String agentId, double value) {
        return new EnrichedSample(new MetricSample(agentId, "pulse.cpu.load", value, AT), "eu-west-1");
    }

    @Test
    @DisplayName("GET /api/metrics/stream : text/event-stream, body.take(2) piloté au StepVerifier")
    void streamsServerSentEvents() {
        when(ingestion.processed()).thenReturn(Flux.just(
                enriched("agent-1", 1.0),
                enriched("agent-2", 2.0),
                enriched("agent-3", 3.0)));

        Flux<EnrichedSample> body = client.get().uri("/api/metrics/stream")
                .accept(MediaType.TEXT_EVENT_STREAM)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().contentTypeCompatibleWith(MediaType.TEXT_EVENT_STREAM)
                .returnResult(EnrichedSample.class)
                .getResponseBody();

        // On ne consomme qu'un préfixe (take(2)) du flux SSE, puis on termine.
        StepVerifier.create(body.take(2))
                .assertNext(es -> assertEquals("agent-1", es.sample().agentId()))
                .assertNext(es -> {
                    assertEquals("agent-2", es.sample().agentId());
                    assertEquals("eu-west-1", es.region());
                })
                .verifyComplete();
    }
}
