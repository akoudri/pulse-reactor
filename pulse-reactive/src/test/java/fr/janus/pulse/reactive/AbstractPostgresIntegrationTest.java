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
        // TODO: câbler spring.r2dbc.url / username / password sur le conteneur Postgres éphémère
    }
}
