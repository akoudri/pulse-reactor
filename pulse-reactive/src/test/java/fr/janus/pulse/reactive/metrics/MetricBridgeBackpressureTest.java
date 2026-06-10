package fr.janus.pulse.reactive.metrics;

import java.time.Instant;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;

import fr.janus.pulse.common.MetricSample;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Sinks;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prouve l'invariant clé du lab J3-2 B : le pont {@link MetricStream} est <strong>borné</strong>.
 * Sous un abonné <em>lent</em> (le cas d'un client SSE qui ne consomme pas assez vite), le
 * tampon ne grandit pas indéfiniment : une fois plein, {@link Sinks.Many#tryEmitNext} refuse
 * les éléments suivants ({@link Sinks.EmitResult#FAIL_OVERFLOW}) au lieu de gonfler la mémoire.
 *
 * <p>Test purement Reactor (pas de contexte Spring ni de broker) : on pilote l'aval à la main.
 */
class MetricBridgeBackpressureTest {

    @Test
    @DisplayName("client lent : au-delà du tampon, le pont borné refuse (FAIL_OVERFLOW) — aucune file non bornée")
    void boundedBridgeRejectsBeyondBufferUnderSlowConsumer() {
        MetricStream stream = new MetricStream();

        // Abonné "SSE lent" : il souscrit mais ne demande RIEN → l'aval ne draine jamais.
        BaseSubscriber<MetricSample> slowClient = new BaseSubscriber<>() {
            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                // Volontairement aucune demande (pas de request) : downstream stalled.
            }
        };
        stream.stream().subscribe(slowClient);

        int flood = MetricStream.BUFFER_SIZE + 64;
        int accepted = 0;
        int overflow = 0;
        for (int i = 0; i < flood; i++) {
            Sinks.EmitResult result = stream.emit(
                    new MetricSample("agent", "pulse.cpu.load", i, Instant.now()));
            if (result == Sinks.EmitResult.OK) {
                accepted++;
            } else if (result == Sinks.EmitResult.FAIL_OVERFLOW) {
                overflow++;
            }
        }
        slowClient.cancel();

        // Borné : on n'a pas pu accepter plus que la capacité du tampon, le surplus est refusé.
        assertTrue(accepted <= MetricStream.BUFFER_SIZE,
                "le pont ne doit pas bufferiser au-delà de sa capacité (accepted=" + accepted + ")");
        assertTrue(overflow > 0,
                "au-delà du tampon, l'émission doit échouer en FAIL_OVERFLOW (overflow=" + overflow + ")");
    }
}
