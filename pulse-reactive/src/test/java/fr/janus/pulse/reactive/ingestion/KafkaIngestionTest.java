package fr.janus.pulse.reactive.ingestion;

import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import fr.janus.pulse.common.MetricSample;
import fr.janus.pulse.reactive.AbstractPostgresIntegrationTest;
import fr.janus.pulse.reactive.context.TraceIdThreadLocalAccessor;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Intégration de bout en bout de l'ingestion Kafka : un message produit via {@link KafkaTemplate}
 * sur le topic {@code metrics} traverse le pont {@code @KafkaListener → Sinks → Flux} puis le
 * pipeline {@code normalize/enrich}, et ressort enrichi sur {@link MetricIngestion#processed()}.
 *
 * <p>Broker Kafka éphémère fourni par le socle {@link AbstractPostgresIntegrationTest} via
 * {@code @ServiceConnection} (lab J4-1 C) : plus de container ni de {@code @DynamicPropertySource}
 * local. On réactive seulement le {@code @KafkaListener} ({@code auto-startup=true}). Aucun
 * {@code reactor-kafka} : Spring Kafka bridgé en {@code Flux}. On attend l'assignation de partition
 * avant de produire ({@code ContainerTestUtils.waitForAssignment}) car la source est <em>hot</em>.
 */
@SpringBootTest
@TestPropertySource(properties = "spring.kafka.listener.auto-startup=true")
class KafkaIngestionTest extends AbstractPostgresIntegrationTest {

    // Type brut : le KafkaTemplate auto-configuré est déclaré KafkaTemplate<?, ?>.
    @SuppressWarnings("rawtypes")
    @Autowired
    private KafkaTemplate template;

    @Autowired
    private MetricSampleCodec codec;

    @Autowired
    private MetricIngestion ingestion;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Value("${pulse.ingestion.topic}")
    private String topic;

    @Test
    @DisplayName("Kafka → @KafkaListener → Sinks → pipeline : un message ressort normalisé + enrichi")
    void messageFlowsThroughBridgeAndPipeline() {
        // Attendre l'assignation de la partition : le consumer doit être prêt avant de produire,
        // sinon le message (auto-offset-reset=latest) pourrait précéder l'abonnement.
        MessageListenerContainer container =
                registry.getListenerContainer(KafkaMetricListener.CONTAINER_ID);
        ContainerTestUtils.waitForAssignment(container, 1);

        MetricSample raw = new MetricSample("agent-1", "cpu.load", 42.0, Instant.now());

        StepVerifier.create(ingestion.processed())
                // .then : produit APRÈS souscription au flux chaud, pour ne rien manquer.
                // Valeur = JSON (codec Jackson 3), comme le producteur applicatif.
                .then(() -> template.send(topic, raw.agentId(), codec.toJson(raw)))
                .assertNext(enriched -> {
                    assertEquals("pulse.cpu.load", enriched.sample().name(), "name normalisé");
                    assertEquals(42.0, enriched.sample().value());
                    assertEquals("eu-west-1", enriched.region(), "région enrichie via l'annuaire");
                })
                .thenCancel()
                .verify(Duration.ofSeconds(20));
    }

    @Test
    @DisplayName("observabilité : le même traceId franchit la frontière de thread Kafka → pipeline → push SSE")
    void sameTraceIdCrossesKafkaToPipelineBoundary() {
        // Capture les logs du pont : "ingéré de Kafka" (thread Kafka) et "poussé vers le flux SSE"
        // (thread boundedElastic après publishOn). Le traceId, écrit dans le Context, doit suivre
        // le saut de thread grâce à la propagation de contexte de J3-1.
        Logger listenerLogger = (Logger) LoggerFactory.getLogger(KafkaMetricListener.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        listenerLogger.addAppender(appender);
        try {
            MessageListenerContainer container =
                    registry.getListenerContainer(KafkaMetricListener.CONTAINER_ID);
            ContainerTestUtils.waitForAssignment(container, 1);

            MetricSample raw = new MetricSample("agent-2", "mem.used", 10.0, Instant.now());
            template.send(topic, raw.agentId(), codec.toJson(raw));

            await().atMost(Duration.ofSeconds(20)).until(() ->
                    eventWith(appender, "ingéré de Kafka") != null
                            && eventWith(appender, "poussé vers le flux SSE") != null);

            String ingestTrace = mdcTrace(eventWith(appender, "ingéré de Kafka"));
            String pushTrace = mdcTrace(eventWith(appender, "poussé vers le flux SSE"));

            assertThat(ingestTrace).as("traceId présent côté ingestion").isNotBlank();
            assertEquals(ingestTrace, pushTrace,
                    "le MÊME traceId doit franchir la frontière de thread (Kafka → pipeline → push SSE)");
        } finally {
            listenerLogger.detachAppender(appender);
        }
    }

    private static ILoggingEvent eventWith(ListAppender<ILoggingEvent> appender, String fragment) {
        return appender.list.stream()
                .filter(e -> e.getFormattedMessage().contains(fragment))
                .findFirst()
                .orElse(null);
    }

    private static String mdcTrace(ILoggingEvent event) {
        return event.getMDCPropertyMap().get(TraceIdThreadLocalAccessor.KEY);
    }
}
