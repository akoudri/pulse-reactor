package fr.janus.pulse.reactive.context;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Génère des identifiants de corrélation ({@code traceId}) <strong>sans appel bloquant</strong>.
 *
 * <p>{@code UUID.randomUUID()} s'appuie sur un {@link java.security.SecureRandom} qui lit
 * l'entropie de l'OS ({@code /dev/urandom} via {@code FileInputStream#readBytes}). Sur un thread
 * <em>non bloquant</em> (event-loop Netty, scheduler réactif), c'est un appel bloquant — détecté
 * par BlockHound (lab J4-1 D) dans le {@code WebFilter} d'entrée et le pont Kafka. Or un
 * {@code traceId} n'a <strong>aucun besoin de robustesse cryptographique</strong> : on construit
 * donc l'UUID (même format 128 bits) à partir de {@link ThreadLocalRandom}, purement en mémoire.
 *
 * <p>On garde la <em>forme</em> UUID (constructeur {@code new UUID(long, long)}, sans RNG) pour ne
 * rien changer au format des identifiants déjà présents dans les logs et le contrat.
 */
public final class TraceIds {

    private TraceIds() {
    }

    /** Nouvel identifiant de corrélation, généré sans RNG cryptographique (non bloquant). */
    public static String newTraceId() {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        return new UUID(rnd.nextLong(), rnd.nextLong()).toString();
    }
}
