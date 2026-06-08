package fr.janus.pulse.common;

/**
 * Échantillon normalisé enrichi de la région de son agent émetteur.
 *
 * @param sample échantillon normalisé d'origine
 * @param region région résolue via l'{@link AgentDirectory} ({@code "unknown"} en repli)
 */
public record EnrichedSample(MetricSample sample, String region) {
}
