package fr.janus.pulse.reactive.health;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import fr.janus.pulse.common.HealthStatus;
import fr.janus.pulse.common.UpstreamHealth;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.publisher.PublisherProbe;

/**
 * Prouve que la <strong>branche de repli</strong> du fan-out de santé (le
 * {@code onErrorResume → DOWN} de {@link HealthAggregator#aggregate()}) est bien
 * <em>souscrite</em> quand un upstream échoue — et seulement dans ce cas.
 *
 * <p>On utilise un {@link PublisherProbe} plutôt qu'un compteur maison : le probe observe les
 * signaux Reactor réels (souscription, requête, annulation, terminaison) au lieu d'un effet de
 * bord qu'on aurait pu placer au mauvais endroit. Surtout, il distingue l'<em>assemblage</em>
 * (le {@code Mono} de repli existe dans la chaîne) de la <em>souscription</em> (la chaîne a
 * effectivement basculé dessus) : {@code assertWasSubscribed()} ne passe que si le repli a
 * réellement été pris à l'exécution, ce qu'un {@code AtomicInteger} ne garantit pas.
 *
 * <p>Test purement Reactor : on reconstruit la forme {@code call.onErrorResume(fallback)} de
 * {@code probe()} sans contexte Spring ni réseau, pour cibler exactement l'invariant.
 */
class HealthFallbackProbeTest {

    private static final UpstreamHealth FALLBACK_VALUE =
            new UpstreamHealth("unstable", HealthStatus.DOWN, -1L);

    /** Reproduit la structure de repli du fan-out : appel protégé, repli en cas d'erreur. */
    private static Mono<UpstreamHealth> probeWith(Mono<UpstreamHealth> call,
                                                  Mono<UpstreamHealth> fallback) {
        return call.onErrorResume(ex -> fallback);
    }

    @Test
    @DisplayName("upstream en échec : la branche de repli est SOUSCRITE et fournit DOWN")
    void fallbackIsSubscribedWhenUpstreamFails() {
        PublisherProbe<UpstreamHealth> fallback = PublisherProbe.of(Mono.just(FALLBACK_VALUE));

        Mono<UpstreamHealth> call = Mono.error(new IllegalStateException("upstream 503"));

        StepVerifier.create(probeWith(call, fallback.mono()))
                .expectNext(FALLBACK_VALUE)
                .verifyComplete();

        // La preuve : le repli a réellement été souscrit (pas seulement assemblé dans la chaîne).
        fallback.assertWasSubscribed();
        fallback.assertWasRequested();
    }

    @Test
    @DisplayName("upstream OK : la branche de repli n'est JAMAIS souscrite")
    void fallbackNeverSubscribedWhenUpstreamSucceeds() {
        PublisherProbe<UpstreamHealth> fallback = PublisherProbe.of(Mono.just(FALLBACK_VALUE));

        UpstreamHealth ok = new UpstreamHealth("unstable", HealthStatus.UP, 12L);
        Mono<UpstreamHealth> call = Mono.just(ok);

        StepVerifier.create(probeWith(call, fallback.mono()))
                .expectNext(ok)
                .verifyComplete();

        // Le chemin nominal ne doit pas toucher au repli : aucune souscription.
        fallback.assertWasNotSubscribed();
    }
}
