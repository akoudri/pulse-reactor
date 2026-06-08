package fr.janus.pulse.common;

import reactor.core.publisher.Mono;

/**
 * Annuaire (potentiellement distant) résolvant la région d'un agent.
 *
 * <p>Contrat seul : l'implémentation réelle (appel HTTP/base) viendra dans un lab
 * ultérieur. Le {@link Mono} retourné est <em>lazy</em> et peut échouer — l'appelant
 * traite l'erreur comme un signal (retry/timeout/repli), jamais avec un {@code block()}.
 */
public interface AgentDirectory {

    /**
     * @param agentId identifiant de l'agent
     * @return la région de l'agent, de façon asynchrone
     */
    Mono<String> regionOf(String agentId);
}
