package fr.janus.pulse.reactive.ingestion;

import java.util.Map;

import org.springframework.stereotype.Component;

import fr.janus.pulse.common.AgentDirectory;
import reactor.core.publisher.Mono;

/**
 * Implémentation en mémoire de l'{@link AgentDirectory} : résout la région d'un agent sans
 * I/O. Suffit pour alimenter l'enrichissement du pipeline (lab J1) côté ingestion temps réel ;
 * un annuaire distant (HTTP/base) le remplacerait sans changer le contrat.
 *
 * <p>Le {@link Mono} reste <em>lazy</em> et ne bloque jamais ({@code Mono.just}), conforme au
 * contrat : l'appelant ({@code IngestionPipeline#enrich}) y applique timeout/retry/repli.
 */
@Component
public class InMemoryAgentDirectory implements AgentDirectory {

    private static final String DEFAULT_REGION = "eu-west-1";

    /** Quelques agents connus ; tout le reste tombe sur la région par défaut. */
    private static final Map<String, String> REGIONS = Map.of(
            "agent-sim", "eu-west-1",
            "agent-1", "eu-west-1",
            "agent-2", "us-east-1",
            "agent-3", "ap-south-1");

    @Override
    public Mono<String> regionOf(String agentId) {
        return Mono.just(REGIONS.getOrDefault(agentId, DEFAULT_REGION));
    }
}
