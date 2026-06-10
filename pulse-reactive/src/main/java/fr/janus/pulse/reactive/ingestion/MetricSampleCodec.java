package fr.janus.pulse.reactive.ingestion;

import org.springframework.stereotype.Component;

import fr.janus.pulse.common.MetricSample;
import tools.jackson.databind.ObjectMapper;

/**
 * (Dé)sérialise un {@link MetricSample} en JSON via le mapper <strong>Jackson 3</strong>
 * auto-configuré par Boot (package {@code tools.jackson}), qui gère {@code java.time} nativement.
 *
 * <p>On passe par ce codec plutôt que par le {@code JsonSerializer} de spring-kafka : ce dernier,
 * sous Boot 4, instancie par défaut un mapper Jackson 2 dépourvu du module {@code jsr310} et
 * échoue donc à sérialiser le champ {@code Instant}. En transportant des {@code String} sur Kafka
 * et en (dé)sérialisant ici, on reste maître du mapper et indépendant de la version Jackson du
 * serializer Kafka.
 */
@Component
public class MetricSampleCodec {

    private final ObjectMapper mapper;

    public MetricSampleCodec(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public String toJson(MetricSample sample) {
        return mapper.writeValueAsString(sample);
    }

    public MetricSample fromJson(String json) {
        return mapper.readValue(json, MetricSample.class);
    }
}
