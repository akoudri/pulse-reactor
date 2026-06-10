package fr.janus.pulse.reactive.ingestion;

import org.springframework.stereotype.Component;

import fr.janus.pulse.common.AgentDirectory;
import fr.janus.pulse.common.EnrichedSample;
import fr.janus.pulse.common.IngestionPipeline;
import fr.janus.pulse.reactive.metrics.MetricStream;
import reactor.core.publisher.Flux;

/**
 * Branche le pont {@link MetricStream} (alimenté par Kafka) sur le <strong>pipeline
 * d'ingestion</strong> des labs J1 : {@code normalize} puis {@code enrich}. Le résultat est
 * un {@link Flux} <em>chaud</em> et partagé que les abonnés aval (SSE en J3-2 B) consomment.
 *
 * <p>Le flux est assemblé une fois (à la construction), pas exécuté : chaque abonné re-souscrit
 * la chaîne {@code normalize/enrich} au-dessus de la source multicast. Rien ne bloque, aucun
 * {@code subscribe()} ici — on retourne le {@code Flux} à l'appelant (assembly-time).
 */
@Component
public class MetricIngestion {

    private final Flux<EnrichedSample> processed;

    public MetricIngestion(MetricStream stream, IngestionPipeline pipeline, AgentDirectory directory) {
        this.processed = pipeline.enrich(pipeline.normalize(stream.stream()), directory);
    }

    /** Flux des échantillons normalisés + enrichis, issus de l'ingestion Kafka. */
    public Flux<EnrichedSample> processed() {
        return processed;
    }
}
