package fr.janus.pulse.mvc.metrics;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Vérifie le SSE du jumeau ({@link org.springframework.web.servlet.mvc.method.annotation.SseEmitter})
 * en intégration : un vrai client HTTP lit le flux {@code text/event-stream} et reçoit les
 * échantillons du simulateur. (MockMvc se prête mal à un flux SSE infini ; on teste donc le
 * comportement réel sur un serveur {@code RANDOM_PORT}.)
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class MetricSseTest {

    @Value("${local.server.port}")
    private int port;

    @Test
    @Timeout(15)
    @DisplayName("le stream SSE délivre les échantillons du simulateur")
    void emitsSseEvents() throws Exception {
        HttpClient http = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/metrics/stream"))
                .header("Accept", "text/event-stream")
                .GET()
                .build();

        HttpResponse<InputStream> response = http.send(request, HttpResponse.BodyHandlers.ofInputStream());

        assertEquals(200, response.statusCode());
        assertTrue(response.headers().firstValue("Content-Type").orElse("").contains("text/event-stream"),
                "le content-type doit être text/event-stream");

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(response.body(), StandardCharsets.UTF_8))) {
            long deadline = System.nanoTime() + Duration.ofSeconds(8).toNanos();
            boolean sawData = false;
            String line;
            while (System.nanoTime() < deadline && (line = reader.readLine()) != null) {
                if (line.startsWith("data:") && line.contains("agent-sim")) {
                    sawData = true;
                    break;
                }
            }
            assertTrue(sawData, "doit recevoir au moins un événement SSE du simulateur");
        }
    }
}
