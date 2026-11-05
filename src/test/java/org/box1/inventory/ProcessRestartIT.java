package org.box1.inventory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;

/** Kills the packaged application with SIGKILL during real work, then restarts it with the prod profile. */
@Tag("containers")
class ProcessRestartIT {
    /** After SIGKILL the broker reassigns partitions only when the dead member's 45 s session expires. */
    private static final Duration AFTER_KILL = Duration.ofSeconds(150);
    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static PostgreSQLContainer<?> postgres;
    private static KafkaContainer kafka;
    private final List<Process> started = new java.util.ArrayList<>();

    /** A failed assertion must not leave a process holding Kafka partitions for the next test. */
    @AfterEach
    void stopEverything() throws InterruptedException {
        for (Process process : started) {
            process.destroyForcibly();
            process.waitFor();
        }
        started.clear();
    }

    @BeforeAll
    static void containers() {
        FullStackResource resource = new FullStackResource();
        resource.start();
        postgres = FullStackResource.postgres;
        kafka = FullStackResource.kafka;
    }

    @AfterAll
    static void stop() {
        kafka.stop();
        postgres.stop();
    }

    private record App(Process process, int port, Path log) {
        void kill() throws InterruptedException {
            process.destroyForcibly();
            process.waitFor();
        }
    }

    private static ProcessBuilder builder(int port, Path log) {
        ProcessBuilder builder = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-jar", Path.of("target", "quarkus-app", "quarkus-run.jar").toString());
        builder.environment().put("QUARKUS_HTTP_PORT", Integer.toString(port));
        builder.environment().put("QUARKUS_DATASOURCE_JDBC_URL", postgres.getJdbcUrl());
        builder.environment().put("QUARKUS_DATASOURCE_USERNAME", postgres.getUsername());
        builder.environment().put("QUARKUS_DATASOURCE_PASSWORD", postgres.getPassword());
        builder.environment().put("KAFKA_BOOTSTRAP_SERVERS", kafka.getBootstrapServers());
        builder.environment().put("INVENTORY_OUTBOX_POLL_INTERVAL", "200ms");
        builder.environment().put("INVENTORY_OUTBOX_LEASE_SECONDS", "5");
        builder.environment().put("INVENTORY_METRICS_REFRESH_INTERVAL", "1s");
        builder.environment().put("INVENTORY_EXPIRY_INTERVAL", "1s");
        builder.redirectErrorStream(true).redirectOutput(log.toFile());
        return builder;
    }

    private App start(Map<String, String> extra) throws Exception {
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        Path log = Files.createTempFile("inventory-service", ".log");
        ProcessBuilder builder = builder(port, log);
        builder.environment().put("INVENTORY_ORDER_EVENTS_FLOOR", Harness.FLOOR);
        builder.environment().putAll(extra);
        App app = new App(builder.start(), port, log);
        started.add(app.process());
        Harness.await("application ready", () -> {
            try {
                return get(app, "/q/health/ready").statusCode() == 200;
            } catch (Exception error) {
                return false;
            }
        });
        return app;
    }

    private static HttpResponse<String> get(App app, String path) throws IOException, InterruptedException {
        return HTTP.send(HttpRequest.newBuilder(URI.create("http://localhost:" + app.port() + path)).build(), HttpResponse.BodyHandlers.ofString());
    }

    private void refusesToStart(Map<String, String> env, String expectedInLog) throws Exception {
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        Path log = Files.createTempFile("inventory-service-refused", ".log");
        ProcessBuilder builder = builder(port, log);
        builder.environment().remove("INVENTORY_ORDER_EVENTS_FLOOR");
        builder.environment().putAll(env);
        Process process = builder.start();
        started.add(process);
        assertTrue(process.waitFor(90, TimeUnit.SECONDS), "exits by itself");
        assertNotEquals(0, process.exitValue());
        assertTrue(Files.readString(log).contains(expectedInLog), "says what is wrong");
    }

    @Test
    void withoutAValidFloorTheApplicationRefusesToStart() throws Exception {
        refusesToStart(Map.of(), "inventory.consumer.order-events-floor");
        refusesToStart(Map.of("INVENTORY_ORDER_EVENTS_FLOOR", "yesterday"), "inventory.consumer.order-events-floor");
        refusesToStart(Map.of("INVENTORY_ORDER_EVENTS_FLOOR", Harness.FLOOR, "INVENTORY_RESERVATION_TTL", "soon"), "inventory.reservation.ttl");
    }

    @Test
    void killDuringMessageProcessingRedeliversAndAppliesOnce() throws Exception {
        App first = start(Map.of());
        String sku = stock(5);
        String orderId = Harness.newOrderId();
        String eventId = Harness.id();
        Harness.exec("CREATE OR REPLACE FUNCTION hang_processed() RETURNS trigger AS $$ BEGIN"
                + " IF NEW.event_id = '" + eventId + "' THEN PERFORM pg_sleep(600); END IF; RETURN NEW; END; $$ LANGUAGE plpgsql");
        Harness.exec("CREATE TRIGGER hang_processed_trigger BEFORE INSERT ON processed_event FOR EACH ROW EXECUTE FUNCTION hang_processed()");
        Harness.send(Harness.ORDER_TOPIC, orderId, Harness.placed(eventId, orderId, "site_nyc", sku, 2));
        Harness.await("the transaction to hang inside the trigger", AFTER_KILL, () -> Harness.count(
                "SELECT count(*) FROM pg_stat_activity WHERE query LIKE 'INSERT INTO processed_event%' AND wait_event = 'PgSleep'") == 1);
        first.kill();
        Harness.exec("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE wait_event = 'PgSleep'");
        Harness.exec("DROP TRIGGER hang_processed_trigger ON processed_event");
        Harness.await("uncommitted effects gone", () -> Harness.count("SELECT count(*) FROM processed_event WHERE event_id = ?::uuid", eventId) == 0);
        App second = start(Map.of());
        try {
            Harness.await("redelivery after restart", AFTER_KILL, () -> "ACTIVE".equals(Harness.status(orderId)));
            assertEquals(2, Harness.reserved("site_nyc", sku));
            assertEquals(1, Harness.outcomes(orderId));
        } finally {
            second.kill();
        }
    }

    @Test
    void crashBetweenCommitAndPublishPublishesAfterRestart() throws Exception {
        App first = start(Map.of());
        String sku = stock(50);
        Harness.pauseBroker();
        App second = null;
        try {
            // Insert the committed state directly: the broker is down, so the consumer cannot deliver it now.
            String orderId = Harness.newOrderId();
            Harness.exec("INSERT INTO outbox (event_id, aggregate_id, event_type, payload, created_at, next_attempt_at) VALUES ('"
                    + Harness.id() + "', '" + orderId + "', 'inventory.reserved.v1', '"
                    + Harness.envelope(Harness.id(), "inventory.reserved.v1", orderId, "2026-11-05T12:00:00Z", Map.of("sku", sku)) + "'::jsonb, now(), now())");
            Harness.await("an active claim", () -> Harness.count("SELECT count(*) FROM outbox WHERE aggregate_id = ? AND claimed_by IS NOT NULL", orderId) == 1);
            first.kill();
            assertEquals(1, Harness.count("SELECT count(*) FROM outbox WHERE aggregate_id = ? AND published_at IS NULL", orderId));
            second = start(Map.of());
            App running = second;
            Harness.await("a failed attempt from the restarted publisher", () -> {
                try {
                    return Files.readString(running.log()).contains("Outbox publish not confirmed");
                } catch (IOException error) {
                    return false;
                }
            });
            Harness.resumeBroker();
            Harness.await("publish after restart", AFTER_KILL, () -> Harness.count("SELECT count(*) FROM outbox WHERE aggregate_id = ? AND published_at IS NOT NULL", orderId) == 1);
            List<JsonNode> events = Harness.published(orderId, Duration.ofSeconds(3));
            assertTrue(events.size() >= 1);
            assertEquals(1, events.stream().map(e -> e.get("eventId").asText()).distinct().count(), "one logical event");
        } finally {
            Harness.resumeBroker();
            if (second != null) {
                second.kill();
            }
        }
    }

    @Test
    void killDuringExpiryExpiresOnceAfterRestart() throws Exception {
        App first = start(Map.of("INVENTORY_RESERVATION_TTL", "PT3S"));
        String sku = stock(5);
        String orderId = Harness.newOrderId();
        Harness.exec("CREATE OR REPLACE FUNCTION hang_expiry() RETURNS trigger AS $$ BEGIN"
                + " IF NEW.to_status = 'EXPIRED' THEN PERFORM pg_sleep(600); END IF; RETURN NEW; END; $$ LANGUAGE plpgsql");
        Harness.exec("CREATE TRIGGER hang_expiry_trigger BEFORE INSERT ON reservation_history FOR EACH ROW EXECUTE FUNCTION hang_expiry()");
        Harness.send(Harness.ORDER_TOPIC, orderId, Harness.placed(Harness.id(), orderId, "site_nyc", sku, 2));
        Harness.await("the expiry to hang", AFTER_KILL, () -> Harness.count(
                "SELECT count(*) FROM pg_stat_activity WHERE query LIKE 'INSERT INTO reservation_history%' AND wait_event = 'PgSleep'") == 1);
        first.kill();
        Harness.exec("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE wait_event = 'PgSleep'");
        Harness.exec("DROP TRIGGER hang_expiry_trigger ON reservation_history");
        Harness.await("uncommitted expiry gone", () -> "ACTIVE".equals(Harness.status(orderId)));
        assertEquals(2, Harness.reserved("site_nyc", sku), "still held");
        App second = start(Map.of("INVENTORY_RESERVATION_TTL", "PT3S"));
        try {
            Harness.await("expiry after restart", AFTER_KILL, () -> "EXPIRED".equals(Harness.status(orderId)));
            assertEquals(0, Harness.reserved("site_nyc", sku));
            assertEquals(1, Harness.outbox(orderId, "inventory.reservation-expired.v1"));
            assertEquals(1, Harness.count("SELECT count(*) FROM reservation_history h JOIN reservations r USING (reservation_id) WHERE r.order_id = ? AND h.to_status = 'EXPIRED'", orderId));
        } finally {
            second.kill();
        }
    }

    @Test
    void prodProfileLogsAreStructuredAndFreeOfCustomerData() throws Exception {
        App app = start(Map.of());
        String sku = stock(5);
        String orderId = Harness.newOrderId();
        Harness.send(Harness.ORDER_TOPIC, orderId, Harness.placed(Harness.id(), orderId, "site_nyc", sku, 2));
        Harness.await("order log line", Duration.ofSeconds(90), () -> {
            try {
                return Files.readString(app.log()).contains(orderId);
            } catch (IOException error) {
                return false;
            }
        });
        app.kill();
        List<String> lines = Files.readAllLines(app.log());
        String line = lines.stream().filter(l -> l.contains(orderId) && l.contains("Event reserved")).findFirst().orElseThrow();
        JsonNode json = Harness.JSON.readTree(line);
        assertEquals(orderId, json.path("mdc").path("orderId").asText());
        assertEquals("corr-test", json.path("mdc").path("correlationId").asText());
        assertEquals("4bf92f3577b34da6a3ce929d0e0e4736", json.path("mdc").path("traceId").asText(), "same trace as the order");
        assertTrue(lines.stream().noneMatch(l -> l.contains("cust_test_secret")), "no customer ID in any log line");
    }

    private static String stock(int onHand) {
        Harness.await("migrations", () -> {
            try {
                return Harness.count("SELECT count(*) FROM inventory_site") >= 1;
            } catch (RuntimeException error) {
                return false;
            }
        });
        return Harness.newSku("site_nyc", onHand);
    }
}
