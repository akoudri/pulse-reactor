package fr.janus.pulse.reactive;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

import reactor.tools.agent.ReactorDebugAgent;

/**
 * Point d'entrée de l'API réactive Pulse (WebFlux, port 8080).
 *
 * <p>L'activation de la propagation automatique de contexte est dans
 * {@link fr.janus.pulse.reactive.context.ContextPropagationConfig} (et non ici dans
 * {@code main}) afin qu'elle s'applique aussi sous {@code @SpringBootTest}, qui n'exécute
 * pas {@code main}.
 *
 * <p><strong>Profiling (lab J4-2 B)</strong> : si {@code -Dpulse.debug-agent=true} est passé,
 * on initialise {@link ReactorDebugAgent} <em>avant</em> tout assemblage de pipeline — il
 * réécrit le bytecode des opérateurs Reactor pour capturer leur point d'assemblage, donnant des
 * stacktraces exploitables quand une erreur surgit loin de son origine dans une chaîne async.
 * Désactivé par défaut (surcoût d'instrumentation) : on ne l'allume que pour une session de
 * profiling, en complément d'un enregistrement JFR.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class PulseReactiveApplication {

    public static void main(String[] args) {
        if (Boolean.getBoolean("pulse.debug-agent")) {
            ReactorDebugAgent.init();
            ReactorDebugAgent.processExistingClasses();
        }
        SpringApplication.run(PulseReactiveApplication.class, args);
    }
}
