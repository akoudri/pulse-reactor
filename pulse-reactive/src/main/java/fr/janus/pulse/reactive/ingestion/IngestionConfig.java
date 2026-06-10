package fr.janus.pulse.reactive.ingestion;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import fr.janus.pulse.common.IngestionPipeline;

/**
 * Câblage de l'ingestion temps réel : le pipeline pur (lab J1) devient un bean réutilisable.
 *
 * <p>On ne déclare <strong>pas</strong> de bean {@code NewTopic} : le broker crée le topic à la
 * première utilisation ({@code auto.create.topics.enable}, vrai par défaut sur apache/kafka comme
 * en test). Surtout, un {@code NewTopic} ferait tenter à {@code KafkaAdmin} une connexion
 * <em>bloquante</em> au démarrage — pénalisant les contextes de test sans broker. Sans topic à
 * créer, {@code KafkaAdmin.initialize()} ne se connecte pas.
 */
@Configuration(proxyBeanMethods = false)
class IngestionConfig {

    /**
     * Le {@link IngestionPipeline} est une classe pure (sans état, sans dépendance Spring) :
     * on l'expose en bean pour l'injecter dans {@code MetricIngestion} plutôt que de le
     * {@code new} au fond du code applicatif.
     */
    @Bean
    IngestionPipeline ingestionPipeline() {
        return new IngestionPipeline();
    }
}
