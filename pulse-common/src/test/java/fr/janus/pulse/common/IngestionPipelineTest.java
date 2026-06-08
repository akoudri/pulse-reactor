package fr.janus.pulse.common;

import java.time.Instant;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

/**
 * Vérifie le contrat de {@link IngestionPipeline#normalize(Flux)} via {@link StepVerifier}.
 *
 * <p>Note pédagogique : aucune donnée n'est produite tant que {@code StepVerifier}
 * ne souscrit pas. On ne fait jamais {@code collectList().block()} suivi d'assertions —
 * c'est le vérificateur qui pilote la souscription et confirme le flux émis.
 */
class IngestionPipelineTest {

    private static final Instant AT = Instant.parse("2026-06-01T10:00:00Z");

    private final IngestionPipeline pipeline = new IngestionPipeline();

    @Test
    @DisplayName("un échantillon valide est arrondi et préfixé")
    void normalizesValidSample() {
        Flux<MetricSample> raw = Flux.just(
                new MetricSample("agent-1", "cpu.load", 0.123456, AT));

        StepVerifier.create(pipeline.normalize(raw))
                .expectNext(new MetricSample("agent-1", "pulse.cpu.load", 0.12, AT))
                .verifyComplete();
    }

    @Test
    @DisplayName("un échantillon de valeur négative est filtré")
    void filtersNegativeSample() {
        Flux<MetricSample> raw = Flux.just(
                new MetricSample("agent-1", "cpu.load", -1.0, AT),
                new MetricSample("agent-1", "mem.used", 42.0, AT));

        // Le négatif n'apparaît pas : seul le second échantillon est émis.
        StepVerifier.create(pipeline.normalize(raw))
                .expectNext(new MetricSample("agent-1", "pulse.mem.used", 42.0, AT))
                .verifyComplete();
    }

    @Test
    @DisplayName("un name déjà préfixé n'est pas re-préfixé")
    void doesNotDoublePrefix() {
        Flux<MetricSample> raw = Flux.just(
                new MetricSample("agent-2", "pulse.disk.io", 3.0, AT));

        StepVerifier.create(pipeline.normalize(raw))
                .expectNextMatches(s -> s.name().equals("pulse.disk.io"))
                .verifyComplete();
    }

    @Test
    @DisplayName("le flux complète sans erreur même vide")
    void completesOnEmpty() {
        StepVerifier.create(pipeline.normalize(Flux.empty()))
                .verifyComplete();
    }
}
