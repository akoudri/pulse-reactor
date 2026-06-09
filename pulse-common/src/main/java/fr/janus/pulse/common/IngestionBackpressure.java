package fr.janus.pulse.common;

import java.time.Duration;
import java.time.Instant;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

/**
 * Démonstration du <strong>backpressure</strong> : un producteur rapide ({@code interval})
 * face à un consommateur lent (ici, la demande contrôlée du test).
 *
 * <p>Le backpressure est un protocole de <em>pull</em> : le consommateur demande
 * ({@code request(n)}) et le producteur ne pousse pas plus que demandé. Les opérateurs
 * {@code onBackpressure*} ne servent que lorsque le producteur ne <em>peut pas</em> ralentir
 * (source <em>hot</em> ou cadencée comme {@code interval}).
 *
 * <p><strong>Debug (Partie D)</strong> — ce pipeline pose un {@code checkpoint("ingestion")}
 * pour obtenir, en cas d'erreur, une trace d'assemblage lisible <em>sans coût notable</em>.
 * Pour des traces d'assemblage sur tout le pipeline en développement, activer le
 * <strong>{@code ReactorDebugAgent}</strong> (agent Java, ou {@code ReactorDebugAgent.init()}
 * au démarrage) : instrumentation au chargement des classes, coût d'exécution négligeable.
 * Ne <strong>jamais</strong> utiliser {@code Hooks.onOperatorDebug()} en production : il
 * capture une stacktrace à <em>chaque</em> assemblage d'opérateur → coût prohibitif.
 */
public final class IngestionBackpressure {

    /** Stratégie appliquée au point de mismatch producteur rapide / consommateur lent. */
    public enum Strategy { BUFFER, LATEST, DROP }

    private static final Instant ORIGIN = Instant.parse("2026-06-01T00:00:00Z");

    /**
     * Producteur rapide : un tick toutes les millisecondes, mappé en {@link MetricSample}
     * dont la {@code value} porte l'index du tick (pour pouvoir l'observer dans les tests).
     */
    private Flux<MetricSample> fastProducer() {
        return Flux.interval(Duration.ofMillis(1))
                .map(tick -> new MetricSample("producer", "pulse.tick", tick.doubleValue(), ORIGIN))
                .checkpoint("ingestion");
    }

    /**
     * Applique la stratégie de backpressure <strong>au point de mismatch</strong> (juste
     * avant le consommateur). C'est cette forme que les tests pilotent en demande contrôlée
     * pour rendre chaque stratégie observable et distincte :
     * <ul>
     *   <li>{@code BUFFER} — file non bornée, FIFO : on conserve les <em>premiers</em>
     *       éléments dans l'ordre ;</li>
     *   <li>{@code LATEST} — ne garde que le <em>dernier</em> élément tant que l'aval ne
     *       demande pas : les intermédiaires sont perdus ;</li>
     *   <li>{@code DROP} — jette tout élément qui ne peut être consommé immédiatement : on
     *       ne reçoit que ceux arrivés alors qu'une demande était en cours.</li>
     * </ul>
     */
    public Flux<MetricSample> backpressured(Strategy strategy) {
        Flux<MetricSample> source = fastProducer();
        return switch (strategy) {
            case BUFFER -> source.onBackpressureBuffer();
            case LATEST -> source.onBackpressureLatest();
            case DROP -> source.onBackpressureDrop();
        };
    }

    /**
     * Variante réaliste : la stratégie de backpressure suivie de l'<strong>offload du
     * consommateur lent</strong> sur {@code boundedElastic} via {@code publishOn}. L'aval
     * (le consommateur) s'exécute alors hors du thread du producteur.
     */
    public Flux<MetricSample> offloadedToSlowConsumer(Strategy strategy) {
        return backpressured(strategy).publishOn(Schedulers.boundedElastic());
    }
}
