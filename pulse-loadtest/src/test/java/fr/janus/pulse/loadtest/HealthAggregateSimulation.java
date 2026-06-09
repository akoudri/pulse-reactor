package fr.janus.pulse.loadtest;

import static io.gatling.javaapi.core.CoreDsl.constantUsersPerSec;
import static io.gatling.javaapi.core.CoreDsl.global;
import static io.gatling.javaapi.core.CoreDsl.rampUsersPerSec;
import static io.gatling.javaapi.core.CoreDsl.scenario;
import static io.gatling.javaapi.http.HttpDsl.http;
import static io.gatling.javaapi.http.HttpDsl.status;

import java.time.Duration;

import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import io.gatling.javaapi.http.HttpProtocolBuilder;

/**
 * Simulation différentielle Gatling (DSL Java) sur le cas le plus discriminant : le fan-out
 * {@code GET /api/health/aggregate}, qui attend des upstreams (dont un lent à ~700 ms).
 *
 * <p>On vise la <strong>même URL</strong> sur les deux jumeaux, en changeant uniquement la
 * propriété système {@code pulse.baseUrl} :
 * <ul>
 *   <li>réactif : {@code -Dpulse.baseUrl=http://localhost:8080}</li>
 *   <li>jumeau MVC+Loom : {@code -Dpulse.baseUrl=http://localhost:8081}</li>
 * </ul>
 *
 * <p>Ce qu'on mesure : <strong>débit et p99 sous charge</strong> (pas la latence d'une
 * requête isolée). Sous l'upstream lent, l'écart attendu vient du modèle de threads — le
 * réactif n'immobilise pas un thread par requête en attente ; le jumeau s'appuie sur les
 * virtual threads pour tenir. Le verdict sort des deux rapports HTML, pas d'une intuition.
 */
public class HealthAggregateSimulation extends Simulation {

    private static final String BASE_URL = System.getProperty("pulse.baseUrl", "http://localhost:8080");

    public HealthAggregateSimulation() {
        HttpProtocolBuilder httpProtocol = http
                .baseUrl(BASE_URL)
                .acceptHeader("application/json");

        ScenarioBuilder scenario = scenario("Fan-out /api/health/aggregate")
                .exec(http("GET aggregate")
                        .get("/api/health/aggregate")
                        .check(status().is(200)));

        setUp(
                scenario.injectOpen(
                        // Montée progressive puis plateau, pour observer le comportement
                        // sous charge soutenue (rampe 1→50 req/s sur 30s, puis 50 req/s 30s).
                        rampUsersPerSec(1).to(50).during(Duration.ofSeconds(30)),
                        constantUsersPerSec(50).during(Duration.ofSeconds(30))
                )
        ).protocols(httpProtocol)
                .assertions(
                        // percentile4 = p99 (défaut Gatling). Garde-fous larges, pas des SLO :
                        // l'intérêt est la COMPARAISON des deux rapports, pas un seuil absolu.
                        global().responseTime().percentile4().lt(2000),
                        global().successfulRequests().percent().gt(95.0)
                );
    }
}
