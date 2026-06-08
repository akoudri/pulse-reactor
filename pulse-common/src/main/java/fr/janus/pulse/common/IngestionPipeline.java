package fr.janus.pulse.common;

import java.time.Duration;

import reactor.core.publisher.Flux;
import reactor.util.retry.Retry;

/**
 * Pipeline d'ingestion <em>pur</em> : il décrit une transformation de flux, il ne
 * l'exécute pas. Rien ne se passe tant que personne ne souscrit (assembly-time vs
 * subscription-time) — c'est {@code StepVerifier} qui souscrira dans les tests.
 *
 * <p>Aucun {@code block()}, aucun {@code subscribe()}, aucun effet de bord : on
 * retourne le {@link Flux} transformé à l'appelant.
 */
public final class IngestionPipeline {

    private static final String PREFIX = "pulse.";

    /**
     * Borne de concurrence du fan-out d'enrichissement. Explicite (pas l'illimité par
     * défaut de {@code flatMap}) pour ne pas écrouler l'annuaire distant sous charge.
     */
    private static final int ENRICH_CONCURRENCY = 8;

    /** Repli de région quand l'annuaire reste indisponible après les retries. */
    private static final String UNKNOWN_REGION = "unknown";

    /**
     * Normalise un flux d'échantillons bruts :
     * <ol>
     *   <li>écarte les valeurs négatives (capteur invalide) ;</li>
     *   <li>arrondit {@code value} à 2 décimales ;</li>
     *   <li>préfixe {@code name} par {@code "pulse."} s'il ne l'est pas déjà.</li>
     * </ol>
     *
     * @param raw flux d'échantillons bruts
     * @return flux normalisé (lazy : non souscrit ici)
     */
    public Flux<MetricSample> normalize(Flux<MetricSample> raw) {
        return raw
                .filter(sample -> sample.value() >= 0)
                .map(sample -> new MetricSample(
                        sample.agentId(),
                        prefixed(sample.name()),
                        roundTo2Decimals(sample.value()),
                        sample.at()));
    }

    /**
     * Enrichit chaque échantillon de sa région, résolue via l'{@link AgentDirectory}.
     *
     * <p>Choix <strong>{@code flatMap}</strong> et non {@code concatMap} : l'ordre des
     * {@link EnrichedSample} n'est pas significatif (chaque échantillon est autonome),
     * donc on laisse les appels distants s'entrelacer pour ne pas sérialiser la latence.
     * {@code concatMap} préserverait l'ordre source mais sérialiserait les appels →
     * latence cumulée. La concurrence est <em>bornée</em> ({@link #ENRICH_CONCURRENCY}).
     *
     * <p>Chaque appel est protégé, dans cet ordre :
     * <ol>
     *   <li>{@code timeout(2s)} — appliqué au {@link reactor.core.publisher.Mono} interne,
     *       donc <em>par échantillon</em> (pas sur le flux global) ; un dépassement émet
     *       une erreur qui déclenche le retry ;</li>
     *   <li>{@code retryWhen(Retry.backoff(3, 200ms))} — back-off exponentiel ;</li>
     *   <li>{@code onErrorReturn("unknown")} — repli en dernier recours une fois les
     *       retries épuisés, plutôt que de propager l'erreur (un échantillon non résolu
     *       ne doit pas casser tout le flux).</li>
     * </ol>
     *
     * @param normalized flux d'échantillons déjà normalisés
     * @param directory  annuaire de résolution de région
     * @return flux enrichi (lazy : non souscrit ici)
     */
    public Flux<EnrichedSample> enrich(Flux<MetricSample> normalized, AgentDirectory directory) {
        return normalized.flatMap(sample ->
                        directory.regionOf(sample.agentId())
                                .timeout(Duration.ofSeconds(2))
                                .retryWhen(Retry.backoff(3, Duration.ofMillis(200)))
                                .onErrorReturn(UNKNOWN_REGION)
                                .map(region -> new EnrichedSample(sample, region)),
                ENRICH_CONCURRENCY);
    }

    private static String prefixed(String name) {
        return name.startsWith(PREFIX) ? name : PREFIX + name;
    }

    private static double roundTo2Decimals(double value) {
        return Math.round(value * 100.0) / 100.0;
    }
}
