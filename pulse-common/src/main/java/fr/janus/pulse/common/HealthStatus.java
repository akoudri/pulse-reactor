package fr.janus.pulse.common;

/** État de santé d'un upstream ou de l'agrégat. */
public enum HealthStatus {
    UP,
    DEGRADED,
    DOWN
}
