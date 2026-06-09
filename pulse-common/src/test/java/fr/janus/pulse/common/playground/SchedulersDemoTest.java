package fr.janus.pulse.common.playground;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prouve <em>par assertion</em> (pas seulement par les logs) l'effet des schedulers.
 */
class SchedulersDemoTest {

    private final SchedulersDemo demo = new SchedulersDemo();

    @Test
    @DisplayName("publishOn fait basculer l'aval sur un pool différent de l'amont")
    void publishOnSwitchesPool() {
        Queue<String> beforePool = new ConcurrentLinkedQueue<>();
        Queue<String> afterPool = new ConcurrentLinkedQueue<>();

        StepVerifier.create(demo.aroundPublishOn(beforePool::add, afterPool::add))
                .expectNextCount(3)
                .verifyComplete();

        // L'aval d'un publishOn(parallel) tourne sur le pool parallel-*.
        assertTrue(afterPool.stream().allMatch(name -> name.startsWith("parallel-")),
                "l'aval doit tourner sur le pool parallel, observé : " + afterPool);
        // Amont et aval ne partagent aucun thread : la frontière publishOn a bien changé de pool.
        assertFalse(afterPool.stream().anyMatch(beforePool::contains),
                "amont et aval ne doivent pas partager de thread — amont=" + beforePool + " aval=" + afterPool);
    }

    @Test
    @DisplayName("un appel bloquant simulé est offloadé sur boundedElastic")
    void blockingCallOffloadedToBoundedElastic() {
        StepVerifier.create(demo.blockingCallOffloaded())
                .expectNextMatches(result -> result.contains("boundedElastic-"))
                .verifyComplete();
    }
}
