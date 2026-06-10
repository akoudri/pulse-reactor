package fr.janus.pulse.reactive.alert;

import org.springframework.data.r2dbc.repository.R2dbcRepository;

import reactor.core.publisher.Flux;

/**
 * Repository réactif des alertes. {@link R2dbcRepository} expose les opérations CRUD en
 * {@code Mono}/{@code Flux} (jamais de retour bloquant).
 *
 * <p>{@code findByMetricName} est une <strong>requête dérivée</strong> : Spring Data la
 * traduit en {@code SELECT ... WHERE metric_name = $1} à partir du nom de la propriété.
 */
public interface AlertRepository extends R2dbcRepository<AlertEntity, Long> {

    /** Alertes portant sur une métrique donnée (query method dérivée). */
    Flux<AlertEntity> findByMetricName(String metricName);
}
