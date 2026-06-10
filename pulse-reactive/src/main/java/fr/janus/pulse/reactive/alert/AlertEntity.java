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
 * @param createdBy  utilisateur ayant créé l'alerte (lab J4-2 A), issu du principal lu via
 *                   {@code ReactiveSecurityContextHolder} dans le pipeline de création. Reste
 *                   <em>interne à la persistance</em> : il n'est pas exposé dans le DTO
 *                   {@link fr.janus.pulse.common.Alert} (contrat partagé inchangé), mais sert
 *                   à filtrer « mes alertes » ({@code findByCreatedBy}).
 */
@Table("alert")
public record AlertEntity(
        @Id Long id,
        @Column("metric_name") String metricName,
        @Column("threshold") double threshold,
        @Column("severity") Severity severity,
        @Column("created_at") Instant createdAt,
        @Column("created_by") String createdBy) {

    /** Nouvelle entité non encore persistée (id null → insert), associée à {@code createdBy}. */
    static AlertEntity newAlert(String metricName, double threshold, Severity severity,
                                Instant createdAt, String createdBy) {
        return new AlertEntity(null, metricName, threshold, severity, createdAt, createdBy);
    }
}
