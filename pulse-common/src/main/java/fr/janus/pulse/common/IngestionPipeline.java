package fr.janus.pulse.common;

import reactor.core.publisher.Flux;

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

    private static String prefixed(String name) {
        return name.startsWith(PREFIX) ? name : PREFIX + name;
    }

    private static double roundTo2Decimals(double value) {
        return Math.round(value * 100.0) / 100.0;
    }
}
