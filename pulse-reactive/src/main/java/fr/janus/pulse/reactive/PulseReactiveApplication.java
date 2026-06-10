package fr.janus.pulse.reactive;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * Point d'entrée de l'API réactive Pulse (WebFlux, port 8080).
 *
 * <p>L'activation de la propagation automatique de contexte est dans
 * {@link fr.janus.pulse.reactive.context.ContextPropagationConfig} (et non ici dans
 * {@code main}) afin qu'elle s'applique aussi sous {@code @SpringBootTest}, qui n'exécute
 * pas {@code main}.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class PulseReactiveApplication {

    public static void main(String[] args) {
        SpringApplication.run(PulseReactiveApplication.class, args);
    }
}
