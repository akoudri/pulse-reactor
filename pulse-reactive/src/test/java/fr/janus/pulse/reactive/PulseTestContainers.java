package fr.janus.pulse.reactive;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Conteneurs d'intégration partagés, exposés à Boot via <strong>{@code @ServiceConnection}</strong>
 * (lab J4-1 C). Boot dérive automatiquement les {@code ConnectionDetails} à partir de ces beans :
 * {@code spring.r2dbc.*} depuis le {@link PostgreSQLContainer}, {@code spring.kafka.bootstrap-servers}
 * depuis le {@link ConfluentKafkaContainer} — <strong>plus de {@code @DynamicPropertySource}</strong>
 * pour les propriétés de connexion.
 *
 * <p><strong>Pas de H2 à la place de R2DBC</strong> : on teste contre un vrai PostgreSQL. Un H2 en
 * mode « compatibilité » masquerait le dialecte réel (types {@code TIMESTAMPTZ}, mots réservés comme
 * {@code at}, comportement {@code GROUP BY}, mapping enum↔VARCHAR) et pourrait inventer des
 * bugs absents en prod ou en cacher de réels — le driver et le wire-protocol ne sont pas les mêmes.
 *
 * <p><strong>Singleton partagé</strong> : les conteneurs sont des champs statiques démarrés une
 * seule fois par JVM (nettoyés par Ryuk). Les beans renvoient ces instances, si bien que tous les
 * contextes de test (même mis en cache séparément) réutilisent les <em>mêmes</em> conteneurs — on
 * conserve la rapidité du <em>singleton container pattern</em> tout en passant par {@code @ServiceConnection}.
 *
 * <p>Image Kafka {@code confluentinc/cp-kafka} via {@link ConfluentKafkaContainer} : l'image
 * {@code apache/kafka} échoue au format KRaft avec Testcontainers 2.0.3
 * (« advertised.listeners cannot use 0.0.0.0 ») ; Boot fournit bien une
 * {@code ConfluentKafkaContainerConnectionDetailsFactory} pour ce type.
 */
@TestConfiguration(proxyBeanMethods = false)
public class PulseTestContainers {

    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:17"));

    static final ConfluentKafkaContainer KAFKA =
            new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.2"));

    static {
        // Démarrage unique, partagé entre tous les contextes de test (le stop est géré par Ryuk).
        POSTGRES.start();
        KAFKA.start();
    }

    @Bean
    @ServiceConnection
    PostgreSQLContainer<?> postgresContainer() {
        return POSTGRES;
    }

    @Bean
    @ServiceConnection
    ConfluentKafkaContainer kafkaContainer() {
        return KAFKA;
    }
}
