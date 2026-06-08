package fr.janus.pulse.common;

import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

/**
 * Vérifie {@link IngestionPipeline#enrich(Flux, AgentDirectory)} en <strong>temps
 * virtuel</strong> : les back-off de 200 ms et les latences d'annuaire sont traversés
 * instantanément, sans {@code Thread.sleep}.
 *
 * <p>Note : {@code withVirtualTime} prend un {@code Supplier<Publisher>} (et non un flux
 * déjà construit) car le {@code VirtualTimeScheduler} doit être installé <em>avant</em>
 * l'assemblage du flux temporel, sinon {@code Mono.delay}/{@code Retry.backoff}
 * capteraient le scheduler réel.
 */
class IngestionPipelineEnrichTest {

    private static final Instant AT = Instant.parse("2026-06-01T10:00:00Z");
    private static final MetricSample SAMPLE =
            new MetricSample("agent-1", "pulse.cpu.load", 0.5, AT);

    private final IngestionPipeline pipeline = new IngestionPipeline();

    @Test
    @DisplayName("cas nominal : la région est résolue")
    void resolvesRegion() {
        AgentDirectory directory =
                new FlakyAgentDirectory(0, Duration.ofMillis(50), "eu-west");

        StepVerifier.withVirtualTime(() ->
                        pipeline.enrich(Flux.just(SAMPLE), directory))
                .thenAwait(Duration.ofSeconds(1))
                .expectNext(new EnrichedSample(SAMPLE, "eu-west"))
                .verifyComplete();
    }

    @Test
    @DisplayName("échoue 2 fois puis réussit : le back-off est traversé en temps virtuel")
    void retriesThenSucceeds() {
        AgentDirectory directory =
                new FlakyAgentDirectory(2, Duration.ofMillis(50), "eu-west");

        StepVerifier.withVirtualTime(() ->
                        pipeline.enrich(Flux.just(SAMPLE), directory))
                // Avance la durée des deux back-off (200 ms + 400 ms, + jitter) et des
                // latences ; on est large pour rester robuste au jitter du backoff.
                .thenAwait(Duration.ofSeconds(5))
                .expectNext(new EnrichedSample(SAMPLE, "eu-west"))
                .verifyComplete();
    }

    @Test
    @DisplayName("échoue au-delà du budget de retry : repli sur \"unknown\"")
    void fallsBackToUnknown() {
        // 5 échecs > 3 retries (4 tentatives au total) → jamais résolu → repli.
        AgentDirectory directory =
                new FlakyAgentDirectory(5, Duration.ofMillis(50), "eu-west");

        StepVerifier.withVirtualTime(() ->
                        pipeline.enrich(Flux.just(SAMPLE), directory))
                .thenAwait(Duration.ofSeconds(5))
                .expectNext(new EnrichedSample(SAMPLE, "unknown"))
                .verifyComplete();
    }
}
