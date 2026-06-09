package fr.janus.pulse.mvc;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * Jumeau impératif de Pulse : Spring MVC + virtual threads (port 8081).
 * Même contrat d'API que {@code pulse-reactive}, implémentation bloquante.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class PulseMvcLoomApplication {

    public static void main(String[] args) {
        SpringApplication.run(PulseMvcLoomApplication.class, args);
    }
}
