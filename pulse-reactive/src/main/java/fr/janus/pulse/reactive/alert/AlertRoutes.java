package fr.janus.pulse.reactive.alert;

import static org.springframework.web.reactive.function.server.RequestPredicates.GET;
import static org.springframework.web.reactive.function.server.RouterFunctions.route;

import java.util.Map;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;

/**
 * Même domaine, exposé en <strong>style fonctionnel</strong> ({@code RouterFunction}) pour
 * {@code GET /api/alerts/count} — afin d'illustrer les deux styles WebFlux côte à côte.
 */
@Configuration
public class AlertRoutes {

    @Bean
    public RouterFunction<ServerResponse> alertCountRoute(AlertService service) {
        return route(GET("/api/alerts/count"),
                request -> service.count()
                        .flatMap(count -> ServerResponse.ok().bodyValue(Map.of("count", count))));
    }
}
