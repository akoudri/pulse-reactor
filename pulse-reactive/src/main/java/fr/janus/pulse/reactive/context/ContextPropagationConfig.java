package fr.janus.pulse.reactive.context;

import org.springframework.context.annotation.Configuration;

import io.micrometer.context.ContextRegistry;
import jakarta.annotation.PostConstruct;
import reactor.core.publisher.Hooks;

/**
 * Active la <strong>propagation automatique de contexte</strong> au démarrage du contexte
 * Spring (donc aussi sous {@code @SpringBootTest}, contrairement à un appel dans {@code main}).
 *
 * <ul>
 *   <li>{@code Hooks.enableAutomaticContextPropagation()} : à appeler une fois, tôt — il
 *       n'affecte que les souscriptions suivantes. Couplé à la lib Micrometer
 *       {@code context-propagation}, il fait suivre le Context Reactor dans les ThreadLocal
 *       à chaque changement de thread.</li>
 *   <li>Enregistrement du pont {@link TraceIdThreadLocalAccessor} (clé {@code traceId} ↔ MDC)
 *       dans le {@code ContextRegistry} : c'est lui qui restaure le {@code traceId} dans le
 *       MDC, d'où sa présence dans les logs du contrôleur ET de l'accès R2DBC.</li>
 * </ul>
 */
@Configuration(proxyBeanMethods = false)
public class ContextPropagationConfig {

    @PostConstruct
    void enableContextPropagation() {
        // TODO: activer la propagation automatique (Hooks) et enregistrer le ThreadLocalAccessor
        //       (traceId ↔ MDC) dans le ContextRegistry de Micrometer
    }
}
