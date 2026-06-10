package fr.janus.pulse.reactive.ingestion;

import java.time.Duration;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import fr.janus.pulse.common.MetricSample;
import fr.janus.pulse.reactive.context.TraceIdThreadLocalAccessor;
import fr.janus.pulse.reactive.metrics.MetricStream;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.Context;

/**
 * Pont <strong>Kafka → Flux</strong> : un {@link KafkaListener} (client Java bloquant, sur son
 * thread de conteneur) <em>pousse</em> chaque message dans le {@link MetricStream} (un
 * {@code Sinks} borné). On n'utilise <strong>pas</strong> {@code reactor-kafka} (discontinué)
 * ni les templates réactifs dépréciés : Spring Kafka bridgé en {@code Flux}, à l'état de l'art.
 *
 * <p><strong>Backpressure de bout en bout</strong>. Le poll Kafka donne un backpressure naturel
 * en amont ; il s'arrête au pont {@code Sinks}. Sur tampon plein
 * ({@link Sinks.EmitResult#FAIL_OVERFLOW}), on met la consommation en pause (alternative propre,
 * lab B) plutôt que de gonfler la mémoire ou de perdre des données.
 *
 * <p><strong>Observabilité</strong> (lab D). Chaque échantillon bridgé incrémente un
 * {@code Counter} ({@code pulse.ingestion.samples}). Surtout, on établit un {@code traceId} dans
 * le <strong>Context Reactor</strong> et on traverse une frontière de thread ({@code publishOn})
 * pendant le traitement : grâce à la propagation de contexte de J3-1
 * ({@code Hooks.enableAutomaticContextPropagation()} + {@link TraceIdThreadLocalAccessor}), le
 * même {@code traceId} se retrouve dans le MDC des logs <em>des deux côtés</em> de la frontière —
 * de l'ingestion Kafka jusqu'au push dans le flux qui alimente le SSE. Sans cette propagation, le
 * {@code traceId} (un {@code ThreadLocal}/MDC) serait perdu au saut de thread.
 */
@Component
public class KafkaMetricListener {

    /** Id du conteneur : sert à le piloter (pause/resume) et à attendre l'assignation en test. */
    public static final String CONTAINER_ID = "metrics-ingestion";

    /** Délai avant de retenter le poll après une saturation : laisse l'aval drainer le tampon. */
    private static final Duration RESUME_DELAY = Duration.ofMillis(500);

    private static final Logger log = LoggerFactory.getLogger(KafkaMetricListener.class);

    private final MetricStream stream;
    private final MetricSampleCodec codec;
    private final KafkaListenerEndpointRegistry registry;
    private final Counter ingested;

    public KafkaMetricListener(MetricStream stream, MetricSampleCodec codec,
                               KafkaListenerEndpointRegistry registry, MeterRegistry meters) {
        this.stream = stream;
        this.codec = codec;
        this.registry = registry;
        this.ingested = Counter.builder("pulse.ingestion.samples")
                .description("Échantillons bridgés de Kafka vers le flux temps réel")
                .register(meters);
    }

    @KafkaListener(id = CONTAINER_ID,
            topics = "${pulse.ingestion.topic}",
            groupId = "${spring.kafka.consumer.group-id}")
    public void onMessage(String json, Acknowledgment ack) {
        MetricSample sample = codec.fromJson(json);
        // traceId de corrélation pour CE message (en prod : lu d'un en-tête Kafka stampé par
        // l'agent). On l'écrit dans le Context : il suivra le saut de thread ci-dessous.
        String traceId = UUID.randomUUID().toString();

        // Driver d'ingestion réactif (pas un subscribe « sauvage » de contrôleur) : il traverse
        // une frontière de thread pour matérialiser la propagation du traceId (lab D).
        Mono.just(sample)
                .doFirst(() -> log.info("ingéré de Kafka : agent={} metric={}",
                        sample.agentId(), sample.name()))
                .publishOn(Schedulers.boundedElastic())
                .doOnNext(s -> bridgeToStream(s, ack))
                .contextWrite(Context.of(TraceIdThreadLocalAccessor.KEY, traceId))
                .subscribe();
    }

    /** Émet dans le pont et applique la politique de backpressure / commit. */
    private void bridgeToStream(MetricSample sample, Acknowledgment ack) {
        Sinks.EmitResult result = stream.emit(sample);
        if (result.isSuccess()) {
            ingested.increment();
            // Log de l'autre côté de la frontière de thread : MÊME traceId que "ingéré de Kafka".
            log.info("poussé vers le flux SSE : agent={} (frontière Kafka→pipeline→SSE franchie)",
                    sample.agentId());
            ack.acknowledge(); // at-least-once : on commet après émission réussie.
        } else {
            // Tampon plein : on applique le backpressure vers Kafka (pause + pas d'ack).
            log.warn("emit rejeté ({}) pour agent={} → pause de la consommation Kafka",
                    result, sample.agentId());
            pauseAndScheduleResume();
        }
    }

    private void pauseAndScheduleResume() {
        MessageListenerContainer container = registry.getListenerContainer(CONTAINER_ID);
        if (container == null || container.isPauseRequested()) {
            return; // resume déjà programmé : on n'empile pas les pauses.
        }
        container.pause();
        Mono.delay(RESUME_DELAY)
                .doOnNext(t -> {
                    log.info("resume de la consommation Kafka (tampon drainé)");
                    container.resume();
                })
                .subscribe();
    }
}
