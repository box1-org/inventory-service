package org.box1.inventory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/** Test helpers that talk to the real database and broker. */
final class Harness {
    static final ObjectMapper JSON = new ObjectMapper();
    static final String ORDER_TOPIC = "box1.order.events.v1";
    static final String INVENTORY_TOPIC = "box1.inventory.events.v1";
    static final String FULFILLMENT_TOPIC = "box1.fulfillment.events.v1";
    static final String DLQ_TOPIC = "box1.inventory.dlq.v1";
    static final String FLOOR = "2026-01-01T00:00:00Z";
    static final String BEFORE_FLOOR = "2025-12-31T23:59:59Z";
    static final String TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    private static boolean brokerPaused;

    private Harness() {}

    static void pauseBroker() {
        brokerPaused = true;
        retrying(() -> FullStackResource.kafka.getDockerClient().pauseContainerCmd(FullStackResource.kafka.getContainerId()).exec());
    }

    static void resumeBroker() {
        if (!brokerPaused) {
            return;
        }
        brokerPaused = false;
        retrying(() -> FullStackResource.kafka.getDockerClient().unpauseContainerCmd(FullStackResource.kafka.getContainerId()).exec());
    }

    /** The container client can hold a stale pooled connection after idling, so these commands retry. */
    private static void retrying(Runnable command) {
        RuntimeException last = null;
        for (int attempt = 0; attempt < 5; attempt++) {
            try {
                command.run();
                return;
            } catch (RuntimeException error) {
                last = error;
            }
        }
        throw last;
    }

    static Connection db() throws SQLException {
        return DriverManager.getConnection(FullStackResource.postgres.getJdbcUrl(),
                FullStackResource.postgres.getUsername(), FullStackResource.postgres.getPassword());
    }

    static long count(String sql, Object... args) {
        try (Connection c = db(); PreparedStatement s = c.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) {
                s.setObject(i + 1, args[i]);
            }
            try (ResultSet r = s.executeQuery()) {
                r.next();
                return r.getLong(1);
            }
        } catch (SQLException error) {
            throw new IllegalStateException(error);
        }
    }

    static String text(String sql, Object... args) {
        try (Connection c = db(); PreparedStatement s = c.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) {
                s.setObject(i + 1, args[i]);
            }
            try (ResultSet r = s.executeQuery()) {
                return r.next() ? r.getString(1) : null;
            }
        } catch (SQLException error) {
            throw new IllegalStateException(error);
        }
    }

    static void exec(String sql) {
        try (Connection c = db(); Statement s = c.createStatement()) {
            s.execute(sql);
        } catch (SQLException error) {
            throw new IllegalStateException(error);
        }
    }

    /** A stock row only this test touches, so tests do not depend on each other's holds. */
    static String newSku(String siteId, int onHand) {
        String sku = "SKU-" + UUID.randomUUID();
        exec("INSERT INTO inventory_stock (site_id, sku, on_hand, updated_at) VALUES ('" + siteId + "', '" + sku + "', " + onHand + ", now())");
        return sku;
    }

    static long onHand(String siteId, String sku) {
        return count("SELECT on_hand FROM inventory_stock WHERE site_id = ? AND sku = ?", siteId, sku);
    }

    static long reserved(String siteId, String sku) {
        return count("SELECT reserved FROM inventory_stock WHERE site_id = ? AND sku = ?", siteId, sku);
    }

    static String status(String orderId) {
        return text("SELECT status FROM reservations WHERE order_id = ?", orderId);
    }

    static String reservationId(String orderId) {
        return text("SELECT reservation_id FROM reservations WHERE order_id = ?", orderId);
    }

    static long outcomes(String orderId) {
        return count("SELECT count(*) FROM outbox WHERE aggregate_id = ? AND event_type IN ('inventory.reserved.v1','inventory.rejected.v1')", orderId);
    }

    static long outbox(String orderId, String type) {
        return count("SELECT count(*) FROM outbox WHERE aggregate_id = ? AND event_type = ?", orderId, type);
    }

    static long items(String orderId, String kind) {
        return count("SELECT count(*) FROM reconciliation_item WHERE order_id = ? AND kind = ?", orderId, kind);
    }

    static void awaitProcessed(String eventId) {
        await("event processed", () -> count("SELECT count(*) FROM processed_event WHERE event_id = ?::uuid", eventId) == 1);
    }

    static void await(String what, BooleanSupplier condition) {
        await(what, Duration.ofSeconds(60), condition);
    }

    static void await(String what, Duration limit, BooleanSupplier condition) {
        long deadline = System.nanoTime() + limit.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(error);
            }
        }
        throw new AssertionError("Timed out waiting for " + what);
    }

    static void send(String topic, String key, String value) {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, FullStackResource.kafka.getBootstrapServers());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(config, new StringSerializer(), new StringSerializer())) {
            producer.send(new ProducerRecord<>(topic, key, value)).get();
        } catch (Exception error) {
            throw new IllegalStateException(error);
        }
    }

    /** Read everything currently on a topic with a throwaway consumer group. */
    static List<ConsumerRecord<String, String>> read(String topic, Duration settle) {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, FullStackResource.kafka.getBootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + settle.toNanos();
            while (System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(250)).forEach(records::add);
            }
        }
        return records;
    }

    /** Events published for one order. */
    static List<JsonNode> published(String orderId, Duration settle) {
        List<JsonNode> events = new ArrayList<>();
        for (ConsumerRecord<String, String> record : read(INVENTORY_TOPIC, settle)) {
            if (orderId.equals(record.key())) {
                events.add(parse(record.value()));
            }
        }
        return events;
    }

    static JsonNode parse(String value) {
        try {
            return JSON.readTree(value);
        } catch (JsonProcessingException error) {
            throw new IllegalStateException(error);
        }
    }

    static String envelope(String eventId, String type, String orderId, String occurredAt, Map<String, Object> payload) {
        Map<String, Object> full = new LinkedHashMap<>(payload);
        full.put("orderId", orderId);
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("eventId", eventId);
        envelope.put("eventType", type);
        envelope.put("schemaVersion", 1);
        envelope.put("aggregateId", orderId);
        envelope.put("occurredAt", occurredAt);
        envelope.put("correlationId", "corr-test");
        envelope.put("causationId", "cause-test");
        envelope.put("traceparent", TRACEPARENT);
        envelope.put("payload", full);
        try {
            return JSON.writeValueAsString(envelope);
        } catch (JsonProcessingException error) {
            throw new IllegalStateException(error);
        }
    }

    static String placed(String eventId, String orderId, String siteId, String sku, int quantity) {
        return placedAt(eventId, orderId, siteId, sku, quantity, Instant.now().toString());
    }

    static String placedAt(String eventId, String orderId, String siteId, String sku, int quantity, String occurredAt) {
        return envelope(eventId, "order.placed.v1", orderId, occurredAt, Map.of("customerId", "cust_test_secret",
                "sku", sku, "quantity", quantity, "siteId", siteId));
    }

    static String requested(String eventId, String orderId, String reservationId, String siteId, String sku, int quantity) {
        return envelope(eventId, "fulfillment.requested.v1", orderId, Instant.now().toString(), Map.of(
                "reservationId", reservationId, "fulfillmentRequestId", "ful_" + orderId.substring(4),
                "siteId", siteId, "sku", sku, "quantity", quantity));
    }

    static String shipped(String eventId, String orderId, String shipmentId) {
        return envelope(eventId, "shipment.confirmed.v1", orderId, Instant.now().toString(), Map.of(
                "fulfillmentRequestId", "ful_" + orderId.substring(4), "shipmentId", shipmentId, "shippedAt", Instant.now().toString()));
    }

    static String shipmentFailed(String eventId, String orderId, boolean retryable) {
        return envelope(eventId, "shipment.failed.v1", orderId, Instant.now().toString(), Map.of(
                "fulfillmentRequestId", "ful_" + orderId.substring(4), "retryable", retryable, "reasonCode", "ERP_DOWN"));
    }

    static String stateChanged(String eventId, String orderId, String toState) {
        return envelope(eventId, "order.state-changed.v1", orderId, Instant.now().toString(), Map.of(
                "fromState", "SUBMITTED", "toState", toState, "reasonCode", "TEST", "changedAt", Instant.now().toString()));
    }

    static String id() {
        return UUID.randomUUID().toString();
    }

    static String newOrderId() {
        return "ord_" + UUID.randomUUID().toString().replace("-", "");
    }
}
