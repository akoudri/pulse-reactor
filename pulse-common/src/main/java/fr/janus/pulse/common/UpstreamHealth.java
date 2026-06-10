package fr.janus.pulse.common;

/**
 * Santé d'un upstream interrogé par le fan-out (Partie B du lab).
 *
 * @param name      nom de l'upstream
 * @param status    état observé
 * @param latencyMs latence de réponse en millisecondes
 */
public record UpstreamHealth(String name, HealthStatus status, long latencyMs) {
}
