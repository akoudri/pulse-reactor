package fr.janus.pulse.reactive.context;

import io.micrometer.context.ThreadLocalAccessor;
import org.slf4j.MDC;

/**
 * Pont entre la clé {@code traceId} du <strong>Context Reactor</strong> et le
 * <strong>MDC</strong> de SLF4J, pour la propagation automatique de contexte
 * ({@code Hooks.enableAutomaticContextPropagation()}).
 *
 * <p>Enregistré dans le {@code ContextRegistry} de Micrometer au démarrage, il permet à la
 * lib {@code context-propagation} de <em>restaurer</em> le {@code traceId} dans le MDC à
 * chaque changement de thread du pipeline réactif — sans que le code applicatif ne touche
 * jamais un {@code ThreadLocal} nu. C'est ce qui fait apparaître le {@code traceId} dans les
 * logs ({@code %X{traceId}}) du contrôleur ET de l'accès R2DBC, malgré les sauts de thread.
 */
public final class TraceIdThreadLocalAccessor implements ThreadLocalAccessor<String> {

    /** Clé partagée entre le Context Reactor, le MDC et le pattern de log. */
    public static final String KEY = "traceId";

    @Override
    public Object key() {
        // TODO: renvoyer la clé exposée du Context (traceId)
        return null;
    }

    @Override
    public String getValue() {
        // TODO: lire la valeur courante depuis le MDC
        return null;
    }

    @Override
    public void setValue(String value) {
        // TODO: poser la valeur dans le MDC
    }

    @Override
    public void setValue() {
        // TODO: retirer la valeur du MDC
    }
}
