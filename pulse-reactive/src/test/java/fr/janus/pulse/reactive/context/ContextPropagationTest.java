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
import static org.springframework.web.reactive.function.client.ExchangeFilterFunctions.basicAuthentication;

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
        client = WebTestClient.bindToApplicationContext(context)
                .configureClient()
                .filter(basicAuthentication("user", "password"))
                .build();
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
        String trace = "trace-" + System.nanoTime();

        client.get().uri("/api/alerts/{id}", "999999") // inexistant → 404, mais les logs émettent
                .header(TraceContextFilter.TRACE_HEADER, trace)
                .header(TraceContextFilter.TENANT_HEADER, "acme")
                .exchange()
                .expectStatus().isNotFound();

        boolean controllerLogHasTrace = appender.list.stream().anyMatch(e ->
                e.getMessage().contains("entrée contrôleur") && trace.equals(mdcTrace(e)));
        boolean sqlLogHasTrace = appender.list.stream().anyMatch(e ->
                e.getMessage().contains("avant accès R2DBC") && trace.equals(mdcTrace(e)));
        boolean tenantReadFromContext = appender.list.stream().anyMatch(e ->
                e.getFormattedMessage().contains("tenant=acme"));

        assertTrue(controllerLogHasTrace, "le traceId doit être dans le MDC du log d'entrée du contrôleur");
        assertTrue(sqlLogHasTrace, "le MÊME traceId doit être dans le MDC du log d'accès R2DBC");
        assertTrue(tenantReadFromContext, "le tenant doit avoir été lu explicitement du Context (deferContextual)");
    }

    private static String mdcTrace(ILoggingEvent event) {
        return event.getMDCPropertyMap().get(TraceIdThreadLocalAccessor.KEY);
    }
}
