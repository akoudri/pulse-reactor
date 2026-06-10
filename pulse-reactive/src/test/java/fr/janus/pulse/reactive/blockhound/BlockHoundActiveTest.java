package fr.janus.pulse.reactive.blockhound;

import java.time.Duration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.blockhound.BlockingOperationError;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

/**
 * Prouve que <strong>BlockHound est actif</strong> sur la session de test (lab J4-1 D) : un appel
 * bloquant ({@code Thread.sleep}) exécuté sur un thread <em>non bloquant</em> ({@code Schedulers.parallel()},
 * réservé au CPU-bound) est <strong>intercepté</strong> et fait échouer le pipeline avec une
 * {@link BlockingOperationError}.
 *
 * <p>C'est précisément ce que les autres tests ne prouvent pas : ils vérifient un <em>comportement</em>
 * sur les chemins couverts ; BlockHound garantit, structurellement, qu'<strong>aucun</strong> appel
 * bloquant ne s'exécute sur un pool non bloquant — y compris sur des chemins non testés
 * fonctionnellement. Si ce test cessait d'échouer, c'est que BlockHound ne serait plus installé
 * (régression de la garantie) — il est donc volontairement « inversé » : on attend l'erreur.
 *
 * <p>À l'inverse, le même blocage <em>offloadé sur {@code boundedElastic}</em> (pool dédié au
 * blocant) est autorisé par défaut par BlockHound : aucune allowlist applicative n'est nécessaire
 * pour le pont Kafka (qui bridge sur {@code boundedElastic}) ni pour R2DBC (driver non bloquant).
 */
class BlockHoundActiveTest {

    @Test
    @DisplayName("BlockHound intercepte un Thread.sleep sur Schedulers.parallel() (pool non bloquant)")
    void detectsBlockingCallOnNonBlockingScheduler() {
        Mono<String> blockingOnParallel = Mono.fromCallable(() -> {
                    Thread.sleep(10); // appel bloquant INTERDIT sur un thread non bloquant
                    return "ne devrait jamais être émis";
                })
                .subscribeOn(Schedulers.parallel());

        StepVerifier.create(blockingOnParallel)
                .expectError(BlockingOperationError.class)
                .verify(Duration.ofSeconds(5));
    }

    @Test
    @DisplayName("le même blocage offloadé sur boundedElastic est autorisé (pool dédié au blocant)")
    void allowsBlockingCallOnBoundedElastic() {
        Mono<String> blockingOffloaded = Mono.fromCallable(() -> {
                    Thread.sleep(10); // toléré : boundedElastic n'est pas marqué « non bloquant »
                    return "calculé";
                })
                .subscribeOn(Schedulers.boundedElastic());

        StepVerifier.create(blockingOffloaded)
                .expectNext("calculé")
                .verifyComplete();
    }
}
