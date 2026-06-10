package fr.janus.pulse.reactive.alert;

/**
 * Résultat d'agrégat « nombre d'alertes par métrique » servi par {@code GET /api/alerts/by-metric}.
 *
 * <p>DTO de réponse propre à {@code pulse-reactive} (le jumeau MVC ne l'expose pas) : il
 * illustre le requêtage via {@code DatabaseClient} quand une query method ne suffit pas
 * (agrégation {@code GROUP BY}). Reste donc local au module, hors contrat partagé.
 *
 * @param metricName métrique surveillée
 * @param count      nombre d'alertes définies sur cette métrique
 */
public record MetricAlertCount(String metricName, long count) {
}
