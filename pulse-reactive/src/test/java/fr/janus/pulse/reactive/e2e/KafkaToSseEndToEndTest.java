package fr.janus.pulse.reactive.e2e;

import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import fr.janus.pulse.common.EnrichedSample;
import fr.janus.pulse.common.MetricSample;
import fr.janus.pulse.reactive.AbstractPostgresIntegrationTest;
import fr.janus.pulse.reactive.context.TraceIdThreadLocalAccessor;
import fr.janus.pulse.reactive.ingestion.KafkaMetricListener;
import fr.janus.pulse.reactive.ingestion.MetricSampleCodec;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.web.reactive.function.client.ExchangeFilterFunctions.basicAuthentication;

/**
 * Test <strong>bout-en-bout</strong> (lab J4-1 C) : on publie sur Kafka et on vérifie que le
 * message <strong>ressort sur le flux SSE</strong> {@code /api/metrics/stream}, consommé par un
 * vrai {@link WebTestClient} — Kafka → {@code @KafkaListener} → {@code Sinks} → {@code normalize/enrich}
 * → SSE. Postgres et Kafka viennent du socle via {@code @ServiceConnection} (Testcontainers).
 *
 * <p>Serveur réel ({@code RANDOM_PORT}) : WebFlux ne committe la réponse SSE qu'au premier élément.
 * Groupe consumer dédié ({@code pulse-e2e}) pour ne pas entrer en concurrence avec le listener
 * (groupe {@code pulse-ingestion}) d'un autre contexte de test resté en cache.
 *
 * <p><strong>Source hot, pas de rejeu.</strong> Le pont {@code Sinks} est multicast sans rejeu : un
 * message bridgé <em>avant</em> que l'abonnement SSE ne soit établi est perdu pour ce client. Plutôt
 * que de parier sur le timing exact de l'abonnement HTTP, on <strong>publie en boucle</strong>
 * (intervalle court) pendant la durée du test : dès que l'abonnement SSE est vivant, il capte le
 * prochain échantillon. {@code take(1)} prélève alors le premier reçu.
 *
 * <p><strong>Corrélation du {@code traceId}.</strong> Le {@code traceId} établi dans le {@code Context}
 * Reactor à l'ingestion <em>ne traverse pas</em> le pont {@code Sinks} (frontière hot : Context de
 * l'émetteur ≠ Context de l'abonné SSE). On corrèle donc — comme en J3-2 — via les logs du listener
 * (MDC) : on prouve qu'un <em>même</em> {@code traceId} apparaît côté « ingéré de Kafka » et côté
 * « poussé vers le flux SSE » (de part et d'autre d'un {@code publishOn}). La livraison SSE effective
 * est prouvée séparément par l'assertion {@link WebTestClient}.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = {
        "spring.kafka.listener.auto-startup=true",
        "spring.kafka.consumer.group-id=pulse-e2e"
})
class KafkaToSseEndToEndTest extends AbstractPostgresIntegrationTest {

    @Value("${local.server.port}")
    private int port;

    @SuppressWarnings("rawtypes")
    @Autowired
    private KafkaTemplate template;

    @Autowired
    private MetricSampleCodec codec;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Value("${pulse.ingestion.topic}")
    private String topic;

    @Test
    @DisplayName("publie sur Kafka → ressort sur le SSE (WebTestClient), même traceId corrélé à l'ingestion")
    void kafkaMessageReachesSseStreamWithCorrelatedTraceId() {
        WebTestClient client = WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .responseTimeout(Duration.ofSeconds(30))
                // /api/metrics/stream est sécurisé (lab J4-2 A) → auth HTTP Basic.
                .filter(basicAuthentication("user", "password"))
                .build();

        // Capture les logs du pont pour prouver la corrélation du traceId à l'ingestion.
        Logger listenerLogger = (Logger) LoggerFactory.getLogger(KafkaMetricListener.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        listenerLogger.addAppender(appender);

        // Le consumer doit être assigné avant qu'on commence à produire.
        MessageListenerContainer container =
                registry.getListenerContainer(KafkaMetricListener.CONTAINER_ID);
        ContainerTestUtils.waitForAssignment(container, 1);

        // Producteur répété : publie un échantillon toutes les 200 ms jusqu'à disposal. Il doit
        // démarrer AVANT l'appel exchange() : sur un flux SSE, WebFlux ne committe la réponse (et
        // donc exchange() ne rend la main) qu'au PREMIER élément émis — il faut donc qu'une
        // émission soit déjà en chemin. La répétition garantit qu'un message arrive une fois
        // l'abonnement SSE établi (source hot, multicast sans rejeu).
        // Intervalle sur boundedElastic : KafkaTemplate.send est un appel bloquant (le client
        // Kafka attend les métadonnées) ; l'exécuter sur le scheduler parallel (non bloquant) par
        // défaut de Flux.interval déclencherait BlockHound. On offload donc le blocant — exactement
        // ce que la pile de prod fait (le pont Kafka bridge sur boundedElastic).
        Disposable producer = Flux.interval(Duration.ofMillis(200), Schedulers.boundedElastic())
                .subscribe(tick -> template.send(topic, "agent-e2e",
                        codec.toJson(new MetricSample("agent-e2e", "cpu.load", 77.0, Instant.now()))));
        try {
            // Connexion SSE : corps = flux d'EnrichedSample décodés depuis text/event-stream.
            Flux<EnrichedSample> body = client.get().uri("/api/metrics/stream")
                    .accept(MediaType.TEXT_EVENT_STREAM)
                    .exchange()
                    .expectStatus().isOk()
                    .expectHeader().contentTypeCompatibleWith(MediaType.TEXT_EVENT_STREAM)
                    .returnResult(EnrichedSample.class)
                    .getResponseBody();

            StepVerifier.create(body.take(1))
                    .assertNext(es -> {
                        assertEquals("agent-e2e", es.sample().agentId(), "agent du message Kafka");
                        assertEquals("pulse.cpu.load", es.sample().name(), "name normalisé");
                        assertEquals("eu-west-1", es.region(), "région enrichie (défaut annuaire)");
                    })
                    .expectComplete()
                    .verify(Duration.ofSeconds(30));
        } finally {
            if (producer != null) {
                producer.dispose();
            }
        }

        // Corrélation traceId à l'ingestion : un même traceId franchit le publishOn du listener
        // (présent à la fois côté « ingéré de Kafka » et côté « poussé vers le flux SSE »).
        Set<String> ingestTraces = tracesOf(appender, "ingéré de Kafka");
        Set<String> pushTraces = tracesOf(appender, "poussé vers le flux SSE");
        assertThat(ingestTraces).as("traceId présents à l'ingestion Kafka").isNotEmpty();
        assertThat(ingestTraces)
                .as("un même traceId corrèle l'ingestion Kafka et le push vers le flux SSE")
                .containsAnyElementsOf(pushTraces);
    }

    /** Ensemble des traceId (MDC, non vides) des événements dont le message contient {@code fragment}. */
    private static Set<String> tracesOf(ListAppender<ILoggingEvent> appender, String fragment) {
        return appender.list.stream()
                .filter(e -> e.getFormattedMessage().contains(fragment))
                .map(e -> e.getMDCPropertyMap().get(TraceIdThreadLocalAccessor.KEY))
                .filter(t -> t != null && !t.isBlank())
                .collect(Collectors.toSet());
    }
}
