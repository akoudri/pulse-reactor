package fr.janus.pulse.reactive.metrics;

import org.springframework.stereotype.Component;

import fr.janus.pulse.common.MetricSample;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

/**
 * Pont programmatique <em>hot</em> entre l'ingestion (Kafka, lab J3-2) et les abonnés aval
 * (pipeline + SSE), via l'API {@link Sinks} — jamais un {@code Processor} déprécié.
 * Multicast : chaque abonné reçoit le flux en direct, sans rejeu de l'historique.
 *
 * <p><strong>Borné</strong> ({@link #BUFFER_SIZE}) : c'est l'invariant clé du lab J3-2 B.
 * {@code onBackpressureBuffer(int)} fixe la taille du tampon ; quand l'aval ne consomme pas
 * assez vite et que le tampon est plein, {@link Sinks.Many#tryEmitNext} retourne
 * {@link Sinks.EmitResult#FAIL_OVERFLOW} <em>au lieu</em> de gonfler la mémoire indéfiniment.
 * C'est ce signal que le pont Kafka exploite pour mettre la consommation en pause
 * (cf. {@code KafkaMetricListener}) — aucune file non bornée ne peut se former.
 *
 * <p>Kafka offre un backpressure « naturel » en amont (le poll ne tire que ce qu'on traite) ;
 * ce backpressure s'<em>arrête ici</em>, au pont {@code Sinks}, frontière entre le monde
 * pull de Kafka et le monde push du multicast.
 */
@Component
public class MetricStream {

    /**
     * Borne du tampon du pont. Volontairement modeste : on veut <em>voir</em> l'overflow
     * sous un abonné lent dans les tests, et prouver que la mémoire ne croît pas sans fin.
     */
    static final int BUFFER_SIZE = 256;

    private final Sinks.Many<MetricSample> sink =
            Sinks.many().multicast().onBackpressureBuffer(BUFFER_SIZE);

    /** Flux à exposer aux abonnés (pipeline d'ingestion, contrôleur SSE). */
    public Flux<MetricSample> stream() {
        return sink.asFlux();
    }

    /**
     * Pousse un échantillon dans le pont. Retourne le résultat d'émission : l'appelant
     * <strong>doit</strong> traiter {@link Sinks.EmitResult#FAIL_OVERFLOW} (tampon plein)
     * plutôt que de l'ignorer — c'est le point d'application du backpressure vers Kafka.
     */
    public Sinks.EmitResult emit(MetricSample sample) {
        return sink.tryEmitNext(sample);
    }
}
