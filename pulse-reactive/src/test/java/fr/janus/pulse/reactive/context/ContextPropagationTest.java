package fr.janus.pulse.reactive.context;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.web.reactive.server.WebTestClient;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import fr.janus.pulse.reactive.AbstractPostgresIntegrationTest;
import fr.janus.pulse.reactive.alert.AlertController;
import fr.janus.pulse.reactive.alert.AlertService;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prouve la <strong>propagation de contexte</strong> de bout en bout : un appel HTTP portant
 * {@code X-Trace-Id} voit ce même {@code traceId} apparaître dans le MDC du log d'entrée du
 * contrôleur ET dans celui de l'accès R2DBC — malgré le changement de thread entre l'entrée
 * Netty et le driver R2DBC. Le {@code tenant}, lui, est lu explicitement via le Context.
 *
 * <p>On capture les {@link ILoggingEvent} via un {@link ListAppender} logback branché sur les
 * loggers du contrôleur et du service, puis on inspecte la {@code MDCPropertyMap} de chaque
 * événement (instantané du MDC au moment du log).
 */
@SpringBootTest
class ContextPropagationTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private ApplicationContext context;

    private WebTestClient client;
    private ListAppender<ILoggingEvent> appender;
    private Logger controllerLogger;
    private Logger serviceLogger;

    @BeforeEach
    void setUp() {
        client = WebTestClient.bindToApplicationContext(context).build();
        appender = new ListAppender<>();
        appender.start();
        controllerLogger = (Logger) LoggerFactory.getLogger(AlertController.class);
        serviceLogger = (Logger) LoggerFactory.getLogger(AlertService.class);
        controllerLogger.addAppender(appender);
        serviceLogger.addAppender(appender);
    }

    @AfterEach
    void tearDown() {
        controllerLogger.detachAppender(appender);
        serviceLogger.detachAppender(appender);
    }

    @Test
    @DisplayName("X-Trace-Id corrélé du filtre au log SQL (même traceId, deux threads), tenant lu du Context")
    void correlatesTraceFromFilterToSqlLog() {
        // TODO: GET /api/alerts/{id} avec les headers X-Trace-Id et X-Tenant, puis vérifier
        //       que le MÊME traceId apparaît dans le MDC du log d'entrée du contrôleur ET du
        //       log d'accès R2DBC, et que le tenant a bien été lu du Context (deferContextual)
    }

    private static String mdcTrace(ILoggingEvent event) {
        // TODO: extraire le traceId de la MDCPropertyMap de l'événement de log
        return null;
    }
}
