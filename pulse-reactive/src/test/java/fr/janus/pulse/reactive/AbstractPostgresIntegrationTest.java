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
 * <p>Propriétés <em>métier</em> conservées ici : on éteint les <strong>deux</strong> simulateurs
 * par défaut, sinon ils pollueraient le flux pendant les tests. Attention au piège : il y en a deux,
 * de portées différentes.
 * <ul>
 *   <li>{@code pulse.ingestion.simulator.enabled} — {@code AgentSimulator}, producteur <em>Kafka</em>.</li>
 *   <li>{@code pulse.simulator.enabled} — {@code MetricSimulator}, qui pousse {@code agent-sim}
 *       <strong>directement</strong> dans le {@code MetricStream} (pont hot partagé), en
 *       court-circuitant Kafka. Oublier de l'éteindre fait gagner ses échantillons {@code agent-sim}
 *       contre le message attendu d'un test (symptôme : {@code expected agent-e2e but was agent-sim}).</li>
 * </ul>
 * Les tests qui ont besoin du {@code @KafkaListener} réactivent {@code spring.kafka.listener.auto-startup}
 * localement.
 */
@Import(PulseTestContainers.class)
@TestPropertySource(properties = {
        "pulse.ingestion.simulator.enabled=false",
        "pulse.simulator.enabled=false"
})
public abstract class AbstractPostgresIntegrationTest {
}
