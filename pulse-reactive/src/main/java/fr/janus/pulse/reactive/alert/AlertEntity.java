package fr.janus.pulse.reactive.alert;

import java.time.Instant;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import fr.janus.pulse.common.Severity;

/**
 * Entité de persistance R2DBC de l'alerte, mappée sur la table {@code alert}.
 *
 * <p>Distincte du DTO {@link fr.janus.pulse.common.Alert} de {@code pulse-common} :
 * conformément au contrat du projet, {@code pulse-common} ne contient que des DTO d'API,
 * chaque application a son propre modèle de persistance. Le mapping entité ↔ DTO se fait
 * à la frontière du service ({@link AlertService}).
 *
 * <p>Record immuable : Spring Data R2DBC instancie via le constructeur de persistance et
 * renvoie une nouvelle instance avec l'{@code id} généré après l'insert. L'{@code id} est
 * un {@code Long} auto-incrémenté (identité Postgres) — {@code null} = nouvelle ligne.
 *
 * @param id         clé technique générée par la base (null avant insert)
 * @param metricName métrique surveillée
 * @param threshold  seuil de déclenchement
 * @param severity   sévérité (mappée en VARCHAR ; R2DBC ne fait pas le mapping enum
 *                   « magique » d'Hibernate, la conversion String↔enum passe par le
 *                   ConversionService de Spring)
 * @param createdAt  instant de création
 */
@Table("alert")
public record AlertEntity(
        @Id Long id,
        @Column("metric_name") String metricName,
        @Column("threshold") double threshold,
        @Column("severity") Severity severity,
        @Column("created_at") Instant createdAt) {

    /** Nouvelle entité non encore persistée (id null → insert). */
    static AlertEntity newAlert(String metricName, double threshold, Severity severity, Instant createdAt) {
        return new AlertEntity(null, metricName, threshold, severity, createdAt);
    }
}
