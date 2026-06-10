package fr.janus.pulse.reactive.blockhound;

import reactor.blockhound.BlockHound;
import reactor.blockhound.integration.BlockHoundIntegration;

/**
 * Intégration BlockHound de Pulse (lab J4-1 D), découverte par {@code ServiceLoader} et appliquée
 * automatiquement par {@code blockhound-junit-platform} à l'installation.
 *
 * <p><strong>Allowlist volontairement minimale : une seule entrée, documentée.</strong> Le code
 * <em>applicatif</em> qui bloquait sur un thread non bloquant a été <strong>corrigé</strong>, pas
 * contourné : le {@code traceId} est désormais généré via {@code ThreadLocalRandom}
 * ({@code TraceIds}) au lieu de {@code UUID.randomUUID()}/{@code SecureRandom}. Il ne reste donc à
 * autoriser qu'un blocage de <em>framework</em> inévitable et hors hot path :
 *
 * <ul>
 *   <li><strong>{@code SASLAuthenticationHandler.handleAuthenticationSASL}</strong> (pilote R2DBC
 *       PostgreSQL) : le handshake d'authentification SCRAM génère un <em>nonce</em> client via
 *       {@link java.security.SecureRandom}, qui lit l'entropie de l'OS ({@code /dev/urandom} →
 *       {@code FileInputStream#readBytes}) sur l'event-loop Netty. C'est un appel <strong>unique,
 *       à l'établissement de la connexion</strong> (pas dans le traitement des requêtes), dans du
 *       code tiers qu'on ne peut pas réécrire ; la lecture de {@code /dev/urandom} est non bloquante
 *       en pratique sous Linux. On l'autorise donc explicitement, et <strong>rien d'autre</strong>.</li>
 *   <li><strong>{@code PlatformResourceBundleLocator.loadBundle}</strong> (Hibernate Validator) :
 *       à la <em>première</em> violation de contrainte, l'interpolation du message charge le
 *       {@code ResourceBundle} {@code ValidationMessages} depuis un jar
 *       ({@code RandomAccessFile#readBytes}) — sur l'event-loop. C'est un chargement
 *       <strong>unique puis mis en cache</strong> (les requêtes suivantes ne lisent plus rien),
 *       dans du code tiers. On l'autorise étroitement.</li>
 * </ul>
 *
 * <p>Signal de conception (point à verbaliser) : si cette allowlist devait grossir, ce serait le
 * signe qu'on offload mal le blocant (du bloquant s'invite sur un pool non bloquant) — à corriger
 * côté conception, pas en élargissant l'allowlist.
 */
public class PulseBlockHoundIntegration implements BlockHoundIntegration {

    @Override
    public void applyTo(BlockHound.Builder builder) {
        // 1) Nonce SCRAM du pilote R2DBC PostgreSQL (SecureRandom) — handshake de connexion.
        builder.allowBlockingCallsInside(
                "io.r2dbc.postgresql.authentication.SASLAuthenticationHandler",
                "handleAuthenticationSASL");
        // 2) Chargement (unique, puis caché) du bundle de messages de Bean Validation.
        builder.allowBlockingCallsInside(
                "org.hibernate.validator.resourceloading.PlatformResourceBundleLocator",
                "loadBundle");
    }
}
