package fr.janus.pulse.common;

import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Démontre le <strong>protocole de demande</strong> et la différence entre les trois
 * stratégies de backpressure, de façon déterministe : {@code StepVerifier} à demande
 * initiale 0 + {@code thenRequest}, en temps virtuel (aucun {@code Thread.sleep}).
 */
class IngestionBackpressureTest {

    private final IngestionBackpressure bp = new IngestionBackpressure();

    @Test
    @DisplayName("BUFFER : rien avant thenRequest, puis exactement n éléments, dans l'ordre (FIFO)")
    void bufferDemandProtocol() {
        StepVerifier.withVirtualTime(() -> bp.backpressured(IngestionBackpressure.Strategy.BUFFER), 0)
                .expectSubscription()
                // ~20 ticks sont bufferisés pendant l'avance du temps, AUCUN n'est délivré (demande 0).
                .expectNoEvent(Duration.ofMillis(20))
                .thenRequest(3)
                // FIFO : on reçoit exactement les 3 PREMIERS ticks, dans l'ordre.
                .expectNextMatches(s -> s.value() == 0.0)
                .expectNextMatches(s -> s.value() == 1.0)
                .expectNextMatches(s -> s.value() == 2.0)
                .thenCancel()
                .verify();
    }

    @Test
    @DisplayName("LATEST : on ne reçoit que le dernier émis ; les premiers sont perdus")
    void latestKeepsOnlyMostRecent() {
        StepVerifier.withVirtualTime(() -> bp.backpressured(IngestionBackpressure.Strategy.LATEST), 0)
                .expectSubscription()
                .expectNoEvent(Duration.ofMillis(20))
                .thenRequest(1)
                // Distinct de BUFFER : la 1re valeur reçue n'est PAS 0 mais un tick récent.
                .expectNextMatches(s -> s.value() >= 10.0)
                .thenCancel()
                .verify();
    }

    @Test
    @DisplayName("DROP : les éléments arrivés sans demande sont jetés")
    void dropDiscardsWithoutDemand() {
        StepVerifier.withVirtualTime(() -> bp.backpressured(IngestionBackpressure.Strategy.DROP), 0)
                .expectSubscription()
                // Ticks émis pendant cette fenêtre : jetés (aucune demande en cours).
                .expectNoEvent(Duration.ofMillis(20))
                .thenRequest(1)
                // Après la demande, le prochain tick émis est délivré (donc une valeur élevée, pas 0).
                .thenAwait(Duration.ofMillis(3))
                .expectNextMatches(s -> s.value() >= 10.0)
                .thenCancel()
                .verify();
    }

    @Test
    @DisplayName("offloadedToSlowConsumer délivre l'aval sur boundedElastic")
    void offloadRunsConsumerOnBoundedElastic() {
        Queue<String> threads = new ConcurrentLinkedQueue<>();

        StepVerifier.create(
                        bp.offloadedToSlowConsumer(IngestionBackpressure.Strategy.BUFFER)
                                .doOnNext(s -> threads.add(Thread.currentThread().getName()))
                                .take(3))
                .expectNextCount(3)
                .verifyComplete();

        assertTrue(threads.stream().allMatch(name -> name.startsWith("boundedElastic-")),
                "l'aval offloadé doit tourner sur boundedElastic, observé : " + threads);
    }
}
