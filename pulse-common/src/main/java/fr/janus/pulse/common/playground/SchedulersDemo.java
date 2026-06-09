package fr.janus.pulse.common.playground;

import java.util.function.Consumer;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.Logger;
import reactor.util.Loggers;

/**
 * Démonstrations de l'effet des schedulers sur le thread d'exécution de chaque étape.
 *
 * <p>Rappel des deux opérateurs :
 * <ul>
 *   <li><strong>{@code subscribeOn}</strong> agit sur <em>toute la chaîne en amont</em>,
 *       quel que soit son emplacement (il choisit le thread de souscription).</li>
 *   <li><strong>{@code publishOn}</strong> ne déplace que <em>l'aval</em> de son point
 *       d'insertion ; chaque changement de thread a un coût, on n'en empile pas « au cas où ».</li>
 * </ul>
 */
public final class SchedulersDemo {

    private static final Logger log = Loggers.getLogger(SchedulersDemo.class);

    /** (1) Sans scheduler : tout s'exécute sur le thread qui souscrit. */
    public Flux<Integer> noScheduler() {
        return Flux.range(1, 3)
                .doOnNext(i -> log.info("noScheduler — étape sur {}", Thread.currentThread().getName()));
    }

    /** (2) {@code subscribeOn(boundedElastic)} : déplace TOUTE la chaîne amont sur ce pool. */
    public Flux<Integer> withSubscribeOn() {
        return Flux.range(1, 3)
                .doOnNext(i -> log.info("subscribeOn — étape sur {}", Thread.currentThread().getName()))
                .subscribeOn(Schedulers.boundedElastic());
    }

    /** (3) {@code publishOn(parallel)} au milieu : seul l'AVAL bascule sur parallel. */
    public Flux<Integer> withPublishOn() {
        return Flux.range(1, 3)
                .doOnNext(i -> log.info("avant publishOn sur {}", Thread.currentThread().getName()))
                .publishOn(Schedulers.parallel())
                .doOnNext(i -> log.info("après publishOn sur {}", Thread.currentThread().getName()));
    }

    /** (4) Combinaison : amont sur boundedElastic, aval sur parallel. */
    public Flux<Integer> combined() {
        return Flux.range(1, 3)
                .doOnNext(i -> log.info("amont sur {}", Thread.currentThread().getName()))
                .subscribeOn(Schedulers.boundedElastic())
                .publishOn(Schedulers.parallel())
                .doOnNext(i -> log.info("aval sur {}", Thread.currentThread().getName()));
    }

    /**
     * Variante <em>instrumentée pour le test</em> : capture le nom du thread juste avant
     * et juste après un {@code publishOn(parallel)}, afin de prouver par assertion (et pas
     * seulement par les logs) que l'amont et l'aval ne tournent pas sur le même pool.
     *
     * @param beforePublishOn reçoit le nom du thread de l'étape amont
     * @param afterPublishOn  reçoit le nom du thread de l'étape aval
     */
    public Flux<Integer> aroundPublishOn(Consumer<String> beforePublishOn, Consumer<String> afterPublishOn) {
        return Flux.range(1, 3)
                .doOnNext(i -> beforePublishOn.accept(Thread.currentThread().getName()))
                .publishOn(Schedulers.parallel())
                .doOnNext(i -> afterPublishOn.accept(Thread.currentThread().getName()));
    }

    /**
     * Offload correct d'un appel <strong>bloquant simulé</strong> sur {@code boundedElastic}.
     *
     * <p>Pourquoi pas {@code parallel()} : ce pool a une taille fixe (= nb de cœurs) et est
     * réservé au CPU-bound non bloquant. Y bloquer (sleep / I/O) gèle un de ses rares threads
     * et peut affamer tout le pipeline réactif. {@code boundedElastic} est précisément conçu
     * pour isoler l'attente bloquante sur des threads élastiques dédiés.
     *
     * <p><em>Note BlockHound (J4)</em> : le {@code Thread.sleep} ci-dessous est un blocage
     * <strong>attendu</strong> car isolé sur {@code boundedElastic} ; BlockHound sera configuré
     * pour le tolérer.
     */
    public Mono<String> blockingCallOffloaded() {
        return Mono.fromCallable(() -> {
                    Thread.sleep(20); // appel bloquant simulé
                    return "calculé sur " + Thread.currentThread().getName();
                })
                .subscribeOn(Schedulers.boundedElastic());
    }
}
