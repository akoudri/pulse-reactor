package fr.janus.pulse.reactive;

import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;

/**
 * Socle des tests d'intégration : il importe {@link PulseTestContainers}, qui fournit un
 * PostgreSQL et un Kafka <strong>éphémères</strong> (Testcontainers) câblés à Boot via
 * {@code @ServiceConnection} (lab J4-1 C). Plus aucun {@code @DynamicPropertySource} pour les
 * propriétés de connexion : Boot dérive {@code spring.r2dbc.*} et
 * {@code spring.kafka.bootstrap-servers} des conteneurs.
 *
 * <p>{@code spring.sql.init.mode=always} (application.yml) rejoue {@code schema.sql} sur la base
 * neuve. Tous les {@code @SpringBootTest} héritent de ce socle : l'{@code @Import} et le
 * {@code @TestPropertySource} sont repris via la hiérarchie de classes de test.
 *
 * <p>Seules propriétés <em>métier</em> conservées ici : on éteint les <strong>deux</strong>
 * simulateurs d'agents par défaut, sinon ils publieraient en continu pendant les tests —
 * {@code AgentSimulator} (producteur Kafka, {@code pulse.ingestion.simulator.enabled}) et
 * {@code MetricSimulator} (push direct dans le {@code MetricStream}, {@code pulse.simulator.enabled}).
 * Ce dernier court-circuite Kafka : laissé actif, ses échantillons « agent-sim » polluent le flux
 * chaud partagé et devancent ceux produits par les tests. Les tests qui ont besoin du
 * {@code @KafkaListener} réactivent {@code spring.kafka.listener.auto-startup} localement.
 */
@Import(PulseTestContainers.class)
@TestPropertySource(properties = {
        "pulse.ingestion.simulator.enabled=false",
        "pulse.simulator.enabled=false"
})
public abstract class AbstractPostgresIntegrationTest {
}
