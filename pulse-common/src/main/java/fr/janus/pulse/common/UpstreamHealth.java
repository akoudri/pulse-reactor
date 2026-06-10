package fr.janus.pulse.common;

/**
 * Santé d'un upstream interrogé par le fan-out (Partie B du lab).
 *
 * @param name      nom logique de l'upstream
 * @param status    état mesuré (repli {@link HealthStatus#DOWN} en cas d'échec)
 * @param latencyMs latence observée en millisecondes ({@code -1} si l'appel a échoué)
 */
public record UpstreamHealth(String name, HealthStatus status, long latencyMs) {
}
