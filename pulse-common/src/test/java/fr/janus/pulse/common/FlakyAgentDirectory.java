package fr.janus.pulse.common;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import reactor.core.publisher.Mono;

/**
 * {@link AgentDirectory} de test : échoue les {@code failuresBeforeSuccess} premiers
 * appels (capteur d'annuaire indisponible) puis renvoie {@code region}, avec une latence
 * simulée via {@link Mono#delay(Duration)}.
 *
 * <p>La logique est dans un {@link Mono#defer} : chaque <em>souscription</em> — donc
 * chaque tentative de retry — réincrémente le compteur, ce qui modélise un annuaire qui
 * « guérit » après quelques échecs.
 */
final class FlakyAgentDirectory implements AgentDirectory {

    private final int failuresBeforeSuccess;
    private final Duration latency;
    private final String region;
    private final AtomicInteger attempts = new AtomicInteger();

    FlakyAgentDirectory(int failuresBeforeSuccess, Duration latency, String region) {
        this.failuresBeforeSuccess = failuresBeforeSuccess;
        this.latency = latency;
        this.region = region;
    }

    @Override
    public Mono<String> regionOf(String agentId) {
        return Mono.delay(latency)
                .then(Mono.defer(() -> {
                    int attempt = attempts.incrementAndGet();
                    if (attempt <= failuresBeforeSuccess) {
                        return Mono.error(new IllegalStateException(
                                "annuaire indisponible (tentative " + attempt + ")"));
                    }
                    return Mono.just(region);
                }));
    }

    int attempts() {
        return attempts.get();
    }
}
