package fr.janus.pulse.reactive;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/** Point d'entrée de l'API réactive Pulse (WebFlux, port 8080). */
@SpringBootApplication
@ConfigurationPropertiesScan
public class PulseReactiveApplication {

    public static void main(String[] args) {
        SpringApplication.run(PulseReactiveApplication.class, args);
    }
}
