package fr.janus.pulse.reactive.ingestion;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import fr.janus.pulse.common.MetricSample;
import jakarta.annotation.PreDestroy;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

/**
 * Petit producteur simulant des <strong>agents</strong> : publie des échantillons synthétiques
 * sur le topic Kafka à intervalle régulier, pour alimenter l'ingestion en l'absence de vrais
 * agents. C'est l'amont du pont {@code Kafka → Sinks → Flux} ({@link KafkaMetricListener}).
 *
 * <p>Désactivable via {@code pulse.ingestion.simulator.enabled=false} (les tests pilotent
 * eux-mêmes la production). Le {@code Flux.interval} qui cadence la publication est un
 * <em>driver</em> de source dont on garde le {@link Disposable} pour l'arrêter au shutdown —
 * ce n'est pas un {@code subscribe()} sauvage de contrôleur.
 */
@Component
public class AgentSimulator {

    private static final Duration PERIOD = Duration.ofMillis(500);

    // KafkaTemplate auto-configuré (valeurs String). Injecté en type brut : le bean Boot est
    // déclaré KafkaTemplate<?, ?> et ne satisfait pas une injection au type concret.
    @SuppressWarnings("rawtypes")
    private final KafkaTemplate template;
    private final MetricSampleCodec codec;
    private final String topic;
    private final boolean enabled;
    private final AtomicLong sequence = new AtomicLong();

    private Disposable driver;

    @SuppressWarnings("rawtypes")
    public AgentSimulator(KafkaTemplate template, MetricSampleCodec codec,
                          PulseIngestionProperties properties) {
        this.template = template;
        this.codec = codec;
        this.topic = properties.topic();
        this.enabled = properties.simulator() != null && properties.simulator().enabled();
    }

    @EventListener(ApplicationReadyEvent.class)
    @SuppressWarnings("unchecked")
    public void start() {
        if (!enabled) {
            return;
        }
        this.driver = Flux.interval(PERIOD)
                .map(this::syntheticSample)
                // clé = agentId : ordonnancement par agent au sein d'une partition. Valeur = JSON.
                .subscribe(sample -> template.send(topic, sample.agentId(), codec.toJson(sample)));
    }

    @PreDestroy
    public void stop() {
        if (driver != null) {
            driver.dispose();
        }
    }

    private MetricSample syntheticSample(long tick) {
        long n = sequence.incrementAndGet();
        double value = n % 100;
        return new MetricSample("agent-sim", "pulse.cpu.load", value, Instant.now());
    }
}
