package com.akfc.training.reactive;

import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.BufferOverflowStrategy;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

public class BackPressureExample {

    private static final Instant ORIGIN = Instant.parse("2026-06-01T00:00:00Z");

    /** Cadence de production : une mesure par milliseconde. */
    private static final Duration PRODUCTION_RATE = Duration.ofMillis(1);
    /** Coût de traitement d'une mesure côté consommateur (volontairement lent). */
    private static final Duration PROCESSING_COST = Duration.ofMillis(50);
    /** Petit prefetch pour minimiser le tampon interne de publishOn et rendre les effets visibles vite. */
    private static final int PREFETCH = 8;
    /** Durée d'observation de chaque démo. */
    private static final long DEMO_DURATION_MS = 1_500;

    record MetricSample(String agentId, String name, double value, Instant at) {
    }

    public static void main(String[] args) throws InterruptedException {
        // BUFFER : on amortit les pics dans une file bornée. Au-delà, on choisit
        // quoi jeter (ici les plus anciens). Sécurité mémoire + on garde le flux.
        runDemo("onBackpressureBuffer (file bornée)", buffered());

        // DROP : pas de file, on jette toute mesure que l'aval n'est pas prêt à
        // recevoir. Idéal quand une mesure ratée est sans conséquence.
        runDemo("onBackpressureDrop (on jette le trop-plein)", dropping());

        // LATEST : on ne conserve QUE la dernière valeur connue. Parfait pour de
        // la télémétrie où seule la mesure la plus fraîche compte.
        runDemo("onBackpressureLatest (seule la plus récente compte)", keepingLatest());

        // ERROR : on échoue vite (OverflowException) dès que l'aval décroche.
        // À privilégier quand perdre une donnée n'est pas acceptable.
        runDemo("onBackpressureError (échec immédiat)", failing());
    }

    /**
     * Producteur rapide commun à toutes les démos : une mesure par milliseconde.
     */
    private static Flux<MetricSample> fastProducer() {
        return Flux.interval(PRODUCTION_RATE)
                .map(tick -> new MetricSample("sensor-1", "pulse.tick", tick.doubleValue(), ORIGIN))
                .checkpoint("ingestion");
    }

    /**
     * Stratégie BUFFER : file bornée à 64 éléments. Quand elle déborde, on
     * abandonne les plus anciens ({@code DROP_OLDEST}) et on journalise.
     */
    private static Flux<MetricSample> buffered() {
        AtomicLong overflowed = new AtomicLong();
        return fastProducer()
                .onBackpressureBuffer(
                        64,
                        dropped -> logOverflow("évincée du tampon", dropped, overflowed),
                        BufferOverflowStrategy.DROP_OLDEST);
    }

    /**
     * Stratégie DROP : aucune file. Toute mesure produite alors que l'aval n'a
     * pas de demande en attente est jetée (et comptée).
     */
    private static Flux<MetricSample> dropping() {
        AtomicLong dropped = new AtomicLong();
        return fastProducer()
                .onBackpressureDrop(sample -> logOverflow("jetée", sample, dropped));
    }

    /**
     * Stratégie LATEST : on ne garde que la dernière mesure ; les précédentes
     * non consommées sont silencieusement écrasées.
     */
    private static Flux<MetricSample> keepingLatest() {
        return fastProducer()
                .onBackpressureLatest();
    }

    /**
     * Stratégie ERROR : dès que l'aval ne peut plus suivre, le flux se termine
     * par une OverflowException (échec rapide, propagé à hookOnError).
     */
    private static Flux<MetricSample> failing() {
        return fastProducer()
                .onBackpressureError();
    }

    /**
     * Branche le consommateur lent derrière la frontière asynchrone et observe
     * le flux pendant une courte fenêtre avant de l'annuler.
     */
    private static void runDemo(String title, Flux<MetricSample> regulated) throws InterruptedException {
        System.out.println("\n=== " + title + " ===");
        SlowConsumer consumer = new SlowConsumer();
        regulated
                // Frontière asynchrone : le traitement lent s'exécute sur un thread
                // dédié, sinon le sleep bloquerait l'horloge de l'interval.
                .publishOn(Schedulers.boundedElastic(), PREFETCH)
                .subscribe(consumer);

        Thread.sleep(DEMO_DURATION_MS);
        consumer.dispose();
        System.out.printf("→ produit en ~%d ms ≈ %d mesures, mais seulement %d réellement consommées%n",
                DEMO_DURATION_MS, DEMO_DURATION_MS / PRODUCTION_RATE.toMillis(), consumer.received());
    }

    private static void logOverflow(String action, MetricSample sample, AtomicLong counter) {
        long total = counter.incrementAndGet();
        // On ne journalise qu'au centième pour ne pas noyer la console.
        if (total % 100 == 0) {
            System.out.printf("   [%s] %d mesures (dernière tick=%.0f)%n", action, total, sample.value());
        }
    }

    /**
     * Consommateur représentatif : il demande les mesures une par une
     * ({@code request(1)}) et simule un traitement coûteux (50 ms) avant de
     * réclamer la suivante. Sa lenteur est ce qui crée la pression en amont et
     * déclenche la stratégie de backpressure choisie.
     */
    static final class SlowConsumer extends BaseSubscriber<MetricSample> {

        private final AtomicLong received = new AtomicLong();

        @Override
        protected void hookOnSubscribe(org.reactivestreams.Subscription subscription) {
            // On contrôle nous-mêmes le rythme : une mesure à la fois.
            request(1);
        }

        @Override
        protected void hookOnNext(MetricSample sample) {
            long count = received.incrementAndGet();
            try {
                // Simulation d'un traitement lent (écriture BDD, appel réseau…).
                Thread.sleep(PROCESSING_COST.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (count % 5 == 0) {
                System.out.printf("   consommée n°%d (tick=%.0f)%n", count, sample.value());
            }
            // Prêt pour la suivante : on relance la demande.
            request(1);
        }

        @Override
        protected void hookOnError(Throwable throwable) {
            System.out.printf("   ✗ flux interrompu : %s%n", throwable.getClass().getSimpleName());
        }

        long received() {
            return received.get();
        }
    }

}
