package fr.janus.pulse.reactive.context;

import java.util.UUID;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;

import reactor.core.publisher.Mono;
import reactor.util.context.Context;

/**
 * Filtre d'entrée qui établit le <strong>contexte transverse</strong> de la requête :
 * un {@code traceId} (corrélation) et un {@code tenant}.
 *
 * <p>On les écrit dans le <strong>Context Reactor</strong> via {@code contextWrite}, au plus
 * près de la souscription : le Context circule de l'aval (l'abonné) vers l'amont, donc une
 * écriture ici est visible par tout le pipeline de la requête. Le {@code traceId} rejoint
 * ensuite le MDC automatiquement (cf. {@link TraceIdThreadLocalAccessor}) — on ne pose donc
 * pas de {@code ThreadLocal} nu à la main, qui ne survivrait pas aux changements de thread.
 *
 * <p>Ordre élevé : le filtre s'exécute tôt pour couvrir toute la chaîne.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class TraceContextFilter implements WebFilter {

    public static final String TRACE_HEADER = "X-Trace-Id";
    public static final String TENANT_HEADER = "X-Tenant";
    public static final String TENANT_KEY = "tenant";

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        // TODO: récupérer/générer le traceId (header X-Trace-Id) et le tenant (header X-Tenant)
        // TODO: poursuivre la chaîne en écrivant traceId + tenant dans le Context (contextWrite)
        return null;
    }

    private static String headerOr(ServerWebExchange exchange, String header, java.util.function.Supplier<String> fallback) {
        // TODO: renvoyer la valeur du header si présente, sinon la valeur de repli
        return null;
    }
}
