package fr.janus.pulse.mvc.metrics;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import fr.janus.pulse.common.MetricSample;

/**
 * Équivalent MVC honnête du flux SSE réactif : on tient une liste d'{@link SseEmitter}
 * actifs et on leur pousse les échantillons. Là où le réactif a un seul {@code Flux}
 * multicast, le modèle servlet gère explicitement les connexions ouvertes.
 */
@Component
public class MetricBroadcaster {

    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    /** Enregistre un nouvel abonné SSE (sans timeout serveur). */
    public SseEmitter subscribe() {
        SseEmitter emitter = new SseEmitter(Long.MAX_VALUE);
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));
        emitter.onError(e -> emitters.remove(emitter));
        emitters.add(emitter);
        return emitter;
    }

    /** Pousse un échantillon à tous les abonnés ; retire ceux dont l'envoi échoue. */
    public void broadcast(MetricSample sample) {
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event().name("metric").data(sample));
            } catch (IOException | RuntimeException ex) {
                emitter.completeWithError(ex);
            }
        }
    }
}
