package fr.janus.pulse.reactive;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Socle des tests d'intégration : une base PostgreSQL <strong>éphémère</strong> fournie par
 * Testcontainers, partagée entre les classes de test via le <em>singleton container pattern</em>
 * (démarrée une fois par JVM, nettoyée par Ryuk en fin de run). Aucune base partagée externe.
 *
 * <p>On câble la connexion R2DBC via {@link DynamicPropertySource} (et non {@code @ServiceConnection})
 * pour rester maître de l'URL {@code r2dbc:postgresql://...} et garder un socle réutilisable par
 * héritage, quelles que soient les options {@code @SpringBootTest} de chaque sous-classe.
 * {@code spring.sql.init.mode=always} (application.yml) rejoue {@code schema.sql} sur cette base neuve.
 */
public abstract class AbstractPostgresIntegrationTest {

    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:17"));

    static {
        POSTGRES.start();
    }

    @DynamicPropertySource
    static void r2dbcProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.r2dbc.url", () -> "r2dbc:postgresql://%s:%d/%s".formatted(
                POSTGRES.getHost(), POSTGRES.getFirstMappedPort(), POSTGRES.getDatabaseName()));
        registry.add("spring.r2dbc.username", POSTGRES::getUsername);
        registry.add("spring.r2dbc.password", POSTGRES::getPassword);
        // Le simulateur d'agents (producteur Kafka) reste éteint par défaut en test : sans
        // broker, ses publications bloqueraient. Le test d'ingestion Kafka, qui démarre un
        // broker Testcontainers, produit explicitement via KafkaTemplate.
        registry.add("pulse.ingestion.simulator.enabled", () -> "false");
        // Le simulateur de métriques (MetricSimulator) pousse aussi des échantillons « agent-sim »
        // directement dans le MetricStream (hors Kafka). Éteint en test : les tests pilotent
        // eux-mêmes les émissions du pont (sinon ses échantillons polluent le flux chaud partagé,
        // p. ex. en devançant le message produit par KafkaIngestionTest).
        registry.add("pulse.simulator.enabled", () -> "false");
    }
}
