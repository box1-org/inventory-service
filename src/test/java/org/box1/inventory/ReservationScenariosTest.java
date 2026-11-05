package org.box1.inventory;

import static io.restassured.RestAssured.given;
import static org.box1.inventory.Harness.FULFILLMENT_TOPIC;
import static org.box1.inventory.Harness.ORDER_TOPIC;
import static org.box1.inventory.Harness.count;
import static org.box1.inventory.Harness.id;
import static org.box1.inventory.Harness.newOrderId;
import static org.box1.inventory.Harness.newSku;
import static org.box1.inventory.Harness.onHand;
import static org.box1.inventory.Harness.outbox;
import static org.box1.inventory.Harness.outcomes;
import static org.box1.inventory.Harness.reserved;
import static org.box1.inventory.Harness.send;
import static org.box1.inventory.Harness.status;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.fasterxml.jackson.databind.JsonNode;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.box1.inventory.domain.PoisonEventException;
import org.box1.inventory.messaging.EventParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** The issue's verification scenarios against real PostgreSQL and Kafka, in one running application. */
@QuarkusTest
@ContainersTag
@QuarkusTestResource(TestTokens.class)
class ReservationScenariosTest {
    private static final String SITE = "site_nyc";
    private static final Set<String> ALL = allTypes();

    @Inject
    ReservationService reservations;

    @Inject
    ExpiryScheduler scheduler;

    @Inject
    EventParser parser;

    @Inject
    TestClock clock;

    private static Set<String> allTypes() {
        Set<String> types = new HashSet<>(EventParser.ORDER_TOPIC_TYPES);
        types.addAll(EventParser.FULFILLMENT_TOPIC_TYPES);
        return types;
    }

    @AfterEach
    void cleanUp() {
        Harness.resumeBroker();
        clock.reset();
    }

    private ReservationService.Applied direct(String value) {
        return reservations.apply(parser.parse(value, ALL));
    }

    private static List<ConsumerRecord<String, String>> dlq(String key) {
        List<ConsumerRecord<String, String>> matched = new ArrayList<>();
        for (ConsumerRecord<String, String> record : Harness.read(Harness.DLQ_TOPIC, Duration.ofSeconds(3))) {
            if (key.equals(record.key())) {
                matched.add(record);
            }
        }
        return matched;
    }

    /** Two simultaneous orders for the final unit, through direct concurrent transactions. */
    @Test
    void twoConcurrentOrdersForTheFinalUnit() throws Exception {
        String sku = newSku(SITE, 1);
        for (int round = 0; round < 10; round++) {
            String a = newOrderId();
            String b = newOrderId();
            exec2(placedTask(a, sku, 1), placedTask(b, sku, 1));
            assertEquals(1, reservedOutcomes(a) + reservedOutcomes(b), "one reservation");
            assertEquals(1, rejected(a) + rejected(b), "one rejection");
            assertEquals(1, reserved(SITE, sku));
            assertEquals(1, onHand(SITE, sku));
            String winner = status(a) != null ? a : b;
            // Free the unit for the next round through the real path.
            reservations.release(Harness.reservationId(winner), "subject-test", "round");
            assertEquals(0, reserved(SITE, sku));
        }
    }

    private Callable<ReservationService.Applied> placedTask(String orderId, String sku, int quantity) {
        return () -> direct(Harness.placed(id(), orderId, SITE, sku, quantity));
    }

    private static long reservedOutcomes(String orderId) {
        return outbox(orderId, "inventory.reserved.v1");
    }

    private static long rejected(String orderId) {
        return outbox(orderId, "inventory.rejected.v1");
    }

    private void exec2(Callable<ReservationService.Applied> first, Callable<ReservationService.Applied> second) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            CountDownLatch gate = new CountDownLatch(2);
            Future<ReservationService.Applied> f1 = pool.submit(() -> {
                gate.countDown();
                gate.await();
                return first.call();
            });
            Future<ReservationService.Applied> f2 = pool.submit(() -> {
                gate.countDown();
                gate.await();
                return second.call();
            });
            f1.get();
            f2.get();
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void manyConcurrentOrdersAcrossSitesAndSkus() throws Exception {
        String[] sites = {"site_nyc", "site_bos"};
        List<String[]> rows = new ArrayList<>();
        int stock = 15;
        for (String site : sites) {
            for (int i = 0; i < 3; i++) {
                rows.add(new String[] {site, newSku(site, stock)});
            }
        }
        ExecutorService pool = Executors.newFixedThreadPool(8);
        List<Future<?>> futures = new ArrayList<>();
        List<String> orders = new ArrayList<>();
        try {
            for (int i = 0; i < 240; i++) {
                String[] row = rows.get(i % rows.size());
                String orderId = newOrderId();
                orders.add(orderId);
                futures.add(pool.submit(() -> direct(Harness.placed(id(), orderId, row[0], row[1], 1))));
            }
            for (Future<?> future : futures) {
                future.get();
            }
        } finally {
            pool.shutdownNow();
        }
        for (String[] row : rows) {
            assertEquals(stock, reserved(row[0], row[1]), "every unit taken, none over");
            assertEquals(stock, onHand(row[0], row[1]));
        }
        for (String orderId : orders) {
            assertEquals(1, outcomes(orderId), "one outcome per order");
        }
        assertEquals(0, count("SELECT count(*) FROM inventory_stock WHERE on_hand < reserved"));
    }

    @Test
    void theSameOrderInParallelDecidesOnce() throws Exception {
        String sku = newSku(SITE, 10);
        String orderId = newOrderId();
        // The conflict is retried by the consumer loop. Here a loser's failure is enough, and it must not leave a hold.
        ExecutorService pool = Executors.newFixedThreadPool(4);
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < 4; i++) {
                futures.add(pool.submit(() -> {
                    try {
                        direct(Harness.placed(id(), orderId, SITE, sku, 2));
                    } catch (RuntimeException expected) {
                        // A unique violation: the consumer loop would retry and read the stored decision.
                        assertTrue(expected.getClass().getName().contains("StoreException"), expected.toString());
                    }
                }));
            }
            for (Future<?> future : futures) {
                future.get();
            }
        } finally {
            pool.shutdownNow();
        }
        assertEquals(2, reserved(SITE, sku));
        assertEquals(1, outcomes(orderId));
        assertEquals(1, count("SELECT count(*) FROM reservations WHERE order_id = ?", orderId));
    }

    @Test
    void reservedThroughKafkaPublishesOneContractEvent() {
        String sku = newSku(SITE, 5);
        String orderId = newOrderId();
        String eventId = id();
        send(ORDER_TOPIC, orderId, Harness.placed(eventId, orderId, SITE, sku, 2));
        Harness.await("hold", () -> "ACTIVE".equals(status(orderId)));
        Harness.await("published", () -> count("SELECT count(*) FROM outbox WHERE aggregate_id = ? AND published_at IS NOT NULL", orderId) == 1);
        assertEquals(3, onHand(SITE, sku) - reserved(SITE, sku));
        List<JsonNode> events = Harness.published(orderId, Duration.ofSeconds(3));
        assertEquals(1, events.size());
        JsonNode event = events.get(0);
        assertEquals("inventory.reserved.v1", event.get("eventType").asText());
        assertEquals(orderId, event.get("aggregateId").asText());
        assertEquals(eventId, event.get("causationId").asText());
        assertEquals("corr-test", event.get("correlationId").asText());
        assertTrue(event.get("traceparent").asText().contains("4bf92f3577b34da6a3ce929d0e0e4736"), "same trace as the order");
        assertEquals(Harness.reservationId(orderId), event.get("payload").get("reservationId").asText());
        assertTrue(!event.toString().contains("cust_test_secret"));
    }

    @Test
    void historyAndConsumedStockAreAttributable() {
        String sku = newSku(SITE, 5);
        String orderId = newOrderId();
        String placed = id();
        String commit = id();
        String ship = id();
        direct(Harness.placed(placed, orderId, SITE, sku, 2));
        direct(Harness.requested(commit, orderId, Harness.reservationId(orderId), SITE, sku, 2));
        assertEquals("COMMITTED", status(orderId));
        direct(Harness.shipped(ship, orderId, "shp_1"));
        assertEquals("CONSUMED", status(orderId));
        assertEquals(3, onHand(SITE, sku));
        assertEquals(0, reserved(SITE, sku));
        String reservation = Harness.reservationId(orderId);
        assertEquals(1, count("SELECT count(*) FROM reservation_history WHERE reservation_id = ? AND to_status = 'ACTIVE' AND event_id = ?::uuid", reservation, placed));
        assertEquals(1, count("SELECT count(*) FROM reservation_history WHERE reservation_id = ? AND to_status = 'COMMITTED' AND event_id = ?::uuid", reservation, commit));
        assertEquals(1, count("SELECT count(*) FROM reservation_history WHERE reservation_id = ? AND to_status = 'CONSUMED' AND event_id = ?::uuid", reservation, ship));
        assertEquals(1, count("SELECT count(*) FROM stock_movement WHERE site_id = ? AND sku = ? AND kind = 'SHIPMENT' AND delta = -2", SITE, sku));
    }

    @Test
    void availableCannotGoNegativeEvenByDirectSql() {
        String sku = newSku(SITE, 1);
        assertThrows(IllegalStateException.class, () -> Harness.exec("UPDATE inventory_stock SET reserved = 2 WHERE sku = '" + sku + "'"));
        assertThrows(IllegalStateException.class, () -> Harness.exec("UPDATE inventory_stock SET on_hand = -1 WHERE sku = '" + sku + "'"));
    }

    @Test
    void historyIsImmutable() {
        String sku = newSku(SITE, 1);
        String orderId = newOrderId();
        direct(Harness.placed(id(), orderId, SITE, sku, 1));
        assertThrows(IllegalStateException.class, () -> Harness.exec("DELETE FROM reservation_history WHERE reservation_id = '" + Harness.reservationId(orderId) + "'"));
        assertThrows(IllegalStateException.class, () -> Harness.exec("UPDATE stock_movement SET delta = 0 WHERE kind = 'SEED'"));
    }

    @Test
    void redeliveryOfTheSameEventIdHasNoEffect() {
        String sku = newSku(SITE, 5);
        String orderId = newOrderId();
        String eventId = id();
        String value = Harness.placed(eventId, orderId, SITE, sku, 2);
        for (int i = 0; i < 5; i++) {
            send(ORDER_TOPIC, orderId, value);
        }
        String marker = id();
        send(ORDER_TOPIC, orderId, Harness.stateChanged(marker, orderId, "RESERVED"));
        Harness.awaitProcessed(marker);
        assertEquals(2, reserved(SITE, sku));
        assertEquals(1, outcomes(orderId));
        assertEquals(1, count("SELECT count(*) FROM reservations WHERE order_id = ?", orderId));
    }

    @Test
    void anEquivalentOrderUnderANewEventIdHasNoEffect() {
        String sku = newSku(SITE, 5);
        String orderId = newOrderId();
        direct(Harness.placed(id(), orderId, SITE, sku, 2));
        direct(Harness.placed(id(), orderId, SITE, sku, 2));
        assertEquals(2, reserved(SITE, sku));
        assertEquals(1, outcomes(orderId));
    }

    @Test
    void aRejectedOrderStaysRejectedAfterStockArrives() {
        String sku = newSku(SITE, 1);
        String orderId = newOrderId();
        String first = id();
        assertEquals("rejected", direct(Harness.placed(first, orderId, SITE, sku, 5)).outcome());
        reservations.restock(SITE, sku, 100, "more stock", "key-" + orderId, "subject-test");
        assertEquals("duplicate", direct(Harness.placed(first, orderId, SITE, sku, 5)).outcome());
        assertEquals("duplicate_order", direct(Harness.placed(id(), orderId, SITE, sku, 5)).outcome());
        assertEquals(0, reserved(SITE, sku));
        assertEquals(1, rejected(orderId));
        assertEquals(0, reservedOutcomes(orderId));
    }

    @Test
    void theSameOrderWithADifferentPayloadIsPoisonAndLeavesNoRecord() {
        String sku = newSku(SITE, 5);
        String orderId = newOrderId();
        direct(Harness.placed(id(), orderId, SITE, sku, 2));
        String conflicting = id();
        assertThrows(PoisonEventException.class, () -> direct(Harness.placed(conflicting, orderId, SITE, sku, 3)));
        assertEquals(0, count("SELECT count(*) FROM processed_event WHERE event_id = ?::uuid", conflicting), "the rollback covers dedupe");
        assertEquals(2, reserved(SITE, sku));
    }

    @Test
    void brokerOutageAfterCommitLosesNothing() {
        String sku = newSku(SITE, 5);
        String orderId = newOrderId();
        Harness.pauseBroker();
        direct(Harness.placed(id(), orderId, SITE, sku, 2));
        assertEquals("ACTIVE", status(orderId));
        Harness.await("a failed publish attempt", () -> count("SELECT count(*) FROM outbox WHERE aggregate_id = ? AND last_error IS NOT NULL", orderId) == 1);
        assertEquals(1, count("SELECT count(*) FROM outbox WHERE aggregate_id = ? AND published_at IS NULL", orderId));
        Harness.resumeBroker();
        Harness.await("publish after recovery", Duration.ofSeconds(120), () -> count("SELECT count(*) FROM outbox WHERE aggregate_id = ? AND published_at IS NOT NULL", orderId) == 1);
        List<JsonNode> events = Harness.published(orderId, Duration.ofSeconds(3));
        assertEquals(1, events.stream().map(e -> e.get("eventId").asText()).distinct().count(), "one logical event");
    }

    @Test
    void aPoisonPayloadGoesToTheDeadLetterTopicAndTheNextEventStillRuns() {
        String key = "poison-" + UUID.randomUUID();
        send(ORDER_TOPIC, key, "{not json");
        Harness.await("dead letter", () -> !dlq(key).isEmpty());
        ConsumerRecord<String, String> record = dlq(key).get(0);
        assertEquals("{not json", record.value(), "unchanged");
        assertEquals("Record value is not valid JSON", new String(record.headers().lastHeader("dead-letter-reason").value(), StandardCharsets.UTF_8));
        assertEquals(ORDER_TOPIC, new String(record.headers().lastHeader("dead-letter-topic").value(), StandardCharsets.UTF_8));

        String sku = newSku(SITE, 1);
        String orderId = newOrderId();
        send(ORDER_TOPIC, orderId, Harness.placed(id(), orderId, SITE, sku, 1));
        Harness.await("the next event", () -> "ACTIVE".equals(status(orderId)));
    }

    @Test
    void aSchemaMismatchIsDeadLettered() {
        String sku = newSku(SITE, 1);
        String orderId = newOrderId();
        send(ORDER_TOPIC, orderId, Harness.placed(id(), orderId, SITE, sku, 1).replace("\"schemaVersion\":1", "\"schemaVersion\":2"));
        Harness.await("dead letter", () -> !dlq(orderId).isEmpty());
        assertEquals("Unsupported schemaVersion", new String(dlq(orderId).get(0).headers().lastHeader("dead-letter-reason").value(), StandardCharsets.UTF_8));
        assertEquals(0, reserved(SITE, sku));
    }

    @Test
    void retryExhaustionDeadLettersAndReplayAppliesOnce() throws SQLException {
        String sku = newSku(SITE, 5);
        String orderId = newOrderId();
        String eventId = id();
        try (java.sql.Connection c = Harness.db(); Statement s = c.createStatement()) {
            s.execute("CREATE OR REPLACE FUNCTION fail_processed() RETURNS trigger AS $$ BEGIN"
                    + " IF NEW.event_id = '" + eventId + "' THEN RAISE EXCEPTION 'injected'; END IF; RETURN NEW; END; $$ LANGUAGE plpgsql");
            s.execute("CREATE TRIGGER fail_processed_trigger BEFORE INSERT ON processed_event FOR EACH ROW EXECUTE FUNCTION fail_processed()");
        }
        String value = Harness.placed(eventId, orderId, SITE, sku, 2);
        try {
            send(ORDER_TOPIC, orderId, value);
            Harness.await("dead letter", () -> !dlq(orderId).isEmpty());
        } finally {
            Harness.exec("DROP TRIGGER fail_processed_trigger ON processed_event");
        }
        ConsumerRecord<String, String> record = dlq(orderId).get(0);
        assertEquals("Retries exhausted", new String(record.headers().lastHeader("dead-letter-reason").value(), StandardCharsets.UTF_8));
        assertEquals(value, record.value());
        assertEquals(0, count("SELECT count(*) FROM reservations WHERE order_id = ?", orderId));
        // The runbook's replay: re-produce the record to its source topic.
        send(ORDER_TOPIC, orderId, record.value());
        send(ORDER_TOPIC, orderId, record.value());
        Harness.await("replay", () -> "ACTIVE".equals(status(orderId)));
        Harness.awaitProcessed(eventId);
        String marker = id();
        send(ORDER_TOPIC, orderId, Harness.stateChanged(marker, orderId, "RESERVED"));
        Harness.awaitProcessed(marker);
        assertEquals(2, reserved(SITE, sku));
        assertEquals(1, outcomes(orderId));
    }

    @Test
    void anEventBeforeTheFloorHasNoEffectAndItsOffsetAdvances() {
        String sku = newSku(SITE, 5);
        String orderId = newOrderId();
        String eventId = id();
        send(ORDER_TOPIC, orderId, Harness.placedAt(eventId, orderId, SITE, sku, 2, Harness.BEFORE_FLOOR));
        String marker = id();
        // The same key, so the same partition: the marker is processed only after the older record.
        send(ORDER_TOPIC, orderId, Harness.stateChanged(marker, orderId, "RESERVED"));
        Harness.awaitProcessed(marker);
        assertEquals(0, count("SELECT count(*) FROM processed_event WHERE event_id = ?::uuid", eventId));
        assertEquals(0, count("SELECT count(*) FROM reservations WHERE order_id = ?", orderId));
        assertEquals(0, count("SELECT count(*) FROM outbox WHERE aggregate_id = ?", orderId));
        assertEquals(0, reserved(SITE, sku));
        assertTrue(metricValue("box1_events_consumed_total", "outcome=\"skipped_before_floor\"") >= 1);

        // Replay gets a new record timestamp but keeps occurredAt, so it stays excluded.
        String replayMarker = id();
        send(ORDER_TOPIC, orderId, Harness.placedAt(eventId, orderId, SITE, sku, 2, Harness.BEFORE_FLOOR));
        send(ORDER_TOPIC, orderId, Harness.stateChanged(replayMarker, orderId, "RESERVED"));
        Harness.awaitProcessed(replayMarker);
        assertEquals(0, reserved(SITE, sku));
        assertEquals(0, outcomes(orderId));
    }

    @Test
    void anEventAtTheFloorIsProcessed() {
        String sku = newSku(SITE, 5);
        String orderId = newOrderId();
        send(ORDER_TOPIC, orderId, Harness.placedAt(id(), orderId, SITE, sku, 2, Harness.FLOOR));
        Harness.await("hold", () -> "ACTIVE".equals(status(orderId)));
    }

    private static double metricValue(String name, String label) {
        String body = given().when().get("/q/metrics").asString();
        return body.lines().filter(l -> l.startsWith(name + "{") && l.contains(label))
                .mapToDouble(l -> Double.parseDouble(l.substring(l.lastIndexOf(' ') + 1))).sum();
    }

    @Test
    void clockDrivenExpiryRestoresStockAndPublishesOnce() {
        String sku = newSku(SITE, 5);
        String orderId = newOrderId();
        direct(Harness.placed(id(), orderId, SITE, sku, 2));
        assertEquals(0, expireNow(), "not due yet");
        assertEquals("ACTIVE", status(orderId));
        clock.advance(Duration.ofMinutes(16));
        scheduler.expireDue();
        Harness.await("expired", () -> "EXPIRED".equals(status(orderId)));
        assertEquals(0, reserved(SITE, sku), "the seeded level is restored");
        assertEquals(5, onHand(SITE, sku));
        scheduler.expireDue();
        scheduler.expireDue();
        assertEquals(1, outbox(orderId, "inventory.reservation-expired.v1"), "repeat expiry publishes nothing more");
        Harness.await("expiry published", () -> count("SELECT count(*) FROM outbox WHERE aggregate_id = ? AND event_type = 'inventory.reservation-expired.v1' AND published_at IS NOT NULL", orderId) == 1);
        JsonNode event = Harness.published(orderId, Duration.ofSeconds(3)).stream()
                .filter(e -> e.get("eventType").asText().equals("inventory.reservation-expired.v1")).findFirst().orElseThrow();
        assertEquals(Harness.reservationId(orderId), event.get("payload").get("reservationId").asText());
        Instant expiredAt = Instant.parse(event.get("payload").get("expiredAt").asText());
        assertTrue(expiredAt.isAfter(Instant.now().plus(Duration.ofMinutes(15))), "uses the injected clock");
        assertEquals(1, count("SELECT count(*) FROM reservation_history WHERE reservation_id = ? AND to_status = 'EXPIRED' AND actor = 'scheduler'", Harness.reservationId(orderId)));
    }

    private int expireNow() {
        return scheduler.expireDue();
    }

    @Test
    void aCommittedHoldDoesNotExpire() {
        String sku = newSku(SITE, 5);
        String orderId = newOrderId();
        direct(Harness.placed(id(), orderId, SITE, sku, 2));
        direct(Harness.requested(id(), orderId, Harness.reservationId(orderId), SITE, sku, 2));
        clock.advance(Duration.ofDays(30));
        scheduler.expireDue();
        assertEquals("COMMITTED", status(orderId));
        assertEquals(2, reserved(SITE, sku));
        assertEquals(0, outbox(orderId, "inventory.reservation-expired.v1"));
    }

    @Test
    void expiryRacingFulfillmentHasOneWinner() throws Exception {
        for (int round = 0; round < 20; round++) {
            String sku = newSku(SITE, 5);
            String orderId = newOrderId();
            direct(Harness.placed(id(), orderId, SITE, sku, 2));
            String reservationId = Harness.reservationId(orderId);
            clock.advance(Duration.ofMinutes(16));
            exec2(() -> {
                reservations.expire(reservationId);
                return null;
            }, () -> direct(Harness.requested(id(), orderId, reservationId, SITE, sku, 2)));
            clock.reset();
            String status = status(orderId);
            if ("EXPIRED".equals(status)) {
                assertEquals(0, reserved(SITE, sku));
                assertEquals(1, outbox(orderId, "inventory.reservation-expired.v1"));
                assertEquals(1, Harness.items(orderId, "FULFILLMENT_AFTER_RELEASE"), "the late signal is recorded");
            } else {
                assertEquals("COMMITTED", status);
                assertEquals(2, reserved(SITE, sku));
                assertEquals(0, outbox(orderId, "inventory.reservation-expired.v1"));
            }
        }
    }

    @Test
    void expiryRacingShipmentHasOneWinner() throws Exception {
        for (int round = 0; round < 20; round++) {
            String sku = newSku(SITE, 5);
            String orderId = newOrderId();
            direct(Harness.placed(id(), orderId, SITE, sku, 2));
            String reservationId = Harness.reservationId(orderId);
            clock.advance(Duration.ofMinutes(16));
            exec2(() -> {
                reservations.expire(reservationId);
                return null;
            }, () -> direct(Harness.shipped(id(), orderId, "shp_" + round())));
            clock.reset();
            String status = status(orderId);
            assertEquals(0, reserved(SITE, sku), "no hold left either way");
            assertEquals(3, onHand(SITE, sku), "the shipped units are off the shelf either way");
            if ("EXPIRED".equals(status)) {
                assertEquals(1, count("SELECT count(*) FROM reconciliation_item WHERE order_id = ? AND kind = 'SHIPMENT_AFTER_RELEASE' AND stock_adjusted", orderId));
            } else {
                assertEquals("CONSUMED", status);
                assertEquals(0, outbox(orderId, "inventory.reservation-expired.v1"));
            }
        }
    }

    private static String round() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    @Test
    void aLateShipmentWithNoStockLeftIsRecordedAndChangesNothing() {
        String sku = newSku(SITE, 2);
        String orderId = newOrderId();
        direct(Harness.placed(id(), orderId, SITE, sku, 2));
        clock.advance(Duration.ofMinutes(16));
        scheduler.expireDue();
        clock.reset();
        Harness.await("expired", () -> "EXPIRED".equals(status(orderId)));
        direct(Harness.placed(id(), newOrderId(), SITE, sku, 2));
        assertEquals(2, reserved(SITE, sku));
        direct(Harness.shipped(id(), orderId, "shp_late"));
        assertEquals(2, onHand(SITE, sku), "the other order's hold is protected");
        assertEquals(1, count("SELECT count(*) FROM reconciliation_item WHERE order_id = ? AND kind = 'SHIPMENT_AFTER_RELEASE' AND NOT stock_adjusted", orderId));
        assertEquals("EXPIRED", status(orderId));
    }

    @Test
    void aRepeatedLateShipmentTakesStockOnce() {
        String sku = newSku(SITE, 5);
        String orderId = newOrderId();
        direct(Harness.placed(id(), orderId, SITE, sku, 2));
        clock.advance(Duration.ofMinutes(16));
        scheduler.expireDue();
        clock.reset();
        Harness.await("expired", () -> "EXPIRED".equals(status(orderId)));
        assertEquals("reconciliation", direct(Harness.shipped(id(), orderId, "shp_late")).outcome());
        assertEquals(3, onHand(SITE, sku));
        assertEquals("duplicate_shipment", direct(Harness.shipped(id(), orderId, "shp_late")).outcome(), "the same shipment under a new eventId");
        assertEquals(3, onHand(SITE, sku));
        assertEquals("reconciliation", direct(Harness.shipped(id(), orderId, "shp_other")).outcome());
        assertEquals(3, onHand(SITE, sku), "a different shipment for this hold is for a person, not a second deduction");
        assertEquals(1, Harness.items(orderId, "SHIPMENT_DUPLICATE"));
        assertEquals(1, count("SELECT count(*) FROM stock_movement WHERE site_id = ? AND sku = ? AND kind = 'LATE_SHIPMENT'", SITE, sku));
    }

    @Test
    void aShipmentForAnotherRequestDoesNotConsumeACommittedHold() {
        String sku = newSku(SITE, 5);
        String orderId = newOrderId();
        direct(Harness.placed(id(), orderId, SITE, sku, 2));
        direct(Harness.requested(id(), orderId, Harness.reservationId(orderId), SITE, sku, 2));
        String wrong = Harness.shipped(id(), orderId, "shp_x").replace("\"ful_", "\"ful_other_");
        assertEquals("reconciliation", direct(wrong).outcome());
        assertEquals("COMMITTED", status(orderId));
        assertEquals(5, onHand(SITE, sku));
        assertEquals(1, Harness.items(orderId, "SHIPMENT_REQUEST_MISMATCH"));
    }

    @Test
    void historyRecordsTheTruePreviousStatus() {
        String sku = newSku(SITE, 5);
        String orderId = newOrderId();
        direct(Harness.placed(id(), orderId, SITE, sku, 2));
        direct(Harness.requested(id(), orderId, Harness.reservationId(orderId), SITE, sku, 2));
        direct(Harness.shipped(id(), orderId, "shp_h"));
        assertEquals("COMMITTED", Harness.text("SELECT from_status FROM reservation_history WHERE reservation_id = ? AND to_status = 'CONSUMED'", Harness.reservationId(orderId)));
    }

    @Test
    void anUnavailableDatabaseHoldsPositionWithoutUsingAttempts() {
        String sku = newSku(SITE, 5);
        String orderId = newOrderId();
        String eventId = id();
        Harness.exec("CREATE OR REPLACE FUNCTION unavailable() RETURNS trigger AS $$ BEGIN"
                + " IF NEW.event_id = '" + eventId + "' THEN RAISE EXCEPTION 'injected' USING ERRCODE = '57P03'; END IF; RETURN NEW; END; $$ LANGUAGE plpgsql");
        Harness.exec("CREATE TRIGGER unavailable_trigger BEFORE INSERT ON processed_event FOR EACH ROW EXECUTE FUNCTION unavailable()");
        try {
            double before = metricTotal("box1_inventory_consumer_held_for_database_total");
            send(ORDER_TOPIC, orderId, Harness.placed(eventId, orderId, SITE, sku, 2));
            // The test bound is 5 attempts, so 8 retries prove attempts are not being used up.
            Harness.await("more retries than the attempt bound", () -> metricTotal("box1_inventory_consumer_held_for_database_total") >= before + 8);
            assertTrue(dlq(orderId).isEmpty(), "nothing dead-lettered");
            assertEquals(0, count("SELECT count(*) FROM reservations WHERE order_id = ?", orderId));
        } finally {
            Harness.exec("DROP TRIGGER unavailable_trigger ON processed_event");
        }
        Harness.await("applied after the database returns", () -> "ACTIVE".equals(status(orderId)));
        assertEquals(1, outcomes(orderId));
    }

    private static double metricTotal(String name) {
        return given().when().get("/q/metrics").asString().lines().filter(l -> l.startsWith(name + " ") || l.startsWith(name + "{"))
                .mapToDouble(l -> Double.parseDouble(l.split(" ")[1])).sum();
    }

    @Test
    void aValueTheDatabaseCanNeverStoreIsDeadLetteredAtOnce() {
        String sku = newSku(SITE, 5);
        String orderId = newOrderId();
        String eventId = id();
        Harness.exec("CREATE OR REPLACE FUNCTION bad_value() RETURNS trigger AS $$ BEGIN"
                + " IF NEW.event_id = '" + eventId + "' THEN RAISE EXCEPTION 'injected' USING ERRCODE = '22021'; END IF; RETURN NEW; END; $$ LANGUAGE plpgsql");
        Harness.exec("CREATE TRIGGER bad_value_trigger BEFORE INSERT ON processed_event FOR EACH ROW EXECUTE FUNCTION bad_value()");
        try {
            double before = metricTotal("box1_event_retries_total");
            send(ORDER_TOPIC, orderId, Harness.placed(eventId, orderId, SITE, sku, 2));
            Harness.await("dead letter", () -> !dlq(orderId).isEmpty());
            assertEquals("Value cannot be stored", new String(dlq(orderId).get(0).headers().lastHeader("dead-letter-reason").value(), StandardCharsets.UTF_8));
            assertEquals(before, metricTotal("box1_event_retries_total"), "no retries");
        } finally {
            Harness.exec("DROP TRIGGER bad_value_trigger ON processed_event");
        }
    }

    @Test
    void invalidUtf8IsDeadLettered() {
        String key = "utf8-" + UUID.randomUUID();
        java.util.Properties config = new java.util.Properties();
        config.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, FullStackResource.kafka.getBootstrapServers());
        try (var producer = new org.apache.kafka.clients.producer.KafkaProducer<>(config,
                new org.apache.kafka.common.serialization.StringSerializer(), new org.apache.kafka.common.serialization.ByteArraySerializer())) {
            producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(ORDER_TOPIC, key, new byte[] {'{', (byte) 0xC3, (byte) 0x28, '}'})).get();
        } catch (Exception error) {
            throw new IllegalStateException(error);
        }
        Harness.await("dead letter", () -> !dlq(key).isEmpty());
        assertEquals("Record value is not valid UTF-8", new String(dlq(key).get(0).headers().lastHeader("dead-letter-reason").value(), StandardCharsets.UTF_8));
    }

    @Test
    void shipmentEdgeCasesOpenReconciliationItems() {
        String sku = newSku(SITE, 5);
        String orderId = newOrderId();
        String none = newOrderId();
        assertEquals("reconciliation", direct(Harness.shipped(id(), none, "shp_none")).outcome());
        assertEquals(1, Harness.items(none, "SHIPMENT_WITHOUT_RESERVATION"));
        direct(Harness.placed(id(), orderId, SITE, sku, 2));
        assertEquals("consumed", direct(Harness.shipped(id(), orderId, "shp_a")).outcome(), "a shipment can overtake the signal");
        assertEquals("duplicate_shipment", direct(Harness.shipped(id(), orderId, "shp_a")).outcome());
        assertEquals("reconciliation", direct(Harness.shipped(id(), orderId, "shp_b")).outcome());
        assertEquals(1, Harness.items(orderId, "SHIPMENT_DUPLICATE"));
        assertEquals(3, onHand(SITE, sku), "stock moved once");
        String failed = newOrderId();
        direct(Harness.placed(id(), failed, SITE, sku, 1));
        assertEquals("ignored", direct(Harness.shipmentFailed(id(), failed, true)).outcome());
        assertEquals("reconciliation", direct(Harness.shipmentFailed(id(), failed, false)).outcome());
        assertEquals(1, Harness.items(failed, "SHIPMENT_FAILED"));
        assertEquals("ACTIVE", status(failed), "the hold stays for a person to decide");
    }

    @Test
    void shipmentEventsArriveThroughKafka() {
        String sku = newSku(SITE, 5);
        String orderId = newOrderId();
        direct(Harness.placed(id(), orderId, SITE, sku, 2));
        send(FULFILLMENT_TOPIC, orderId, Harness.shipped(id(), orderId, "shp_kafka"));
        Harness.await("consumed", () -> "CONSUMED".equals(status(orderId)));
        assertEquals(3, onHand(SITE, sku));
    }

    @Test
    void aSignalForAReservationThatIsNotThisOrdersOpensAnItem() {
        String sku = newSku(SITE, 5);
        String orderId = newOrderId();
        String other = newOrderId();
        direct(Harness.placed(id(), orderId, SITE, sku, 2));
        direct(Harness.requested(id(), other, Harness.reservationId(orderId), SITE, sku, 2));
        assertEquals("ACTIVE", status(orderId));
        assertEquals(1, Harness.items(other, "FULFILLMENT_WITHOUT_RESERVATION"));
    }

    @Test
    void cancellationReleasesAnActiveHoldOnly() {
        String sku = newSku(SITE, 5);
        String active = newOrderId();
        direct(Harness.placed(id(), active, SITE, sku, 2));
        assertEquals("released", direct(Harness.stateChanged(id(), active, "CANCELLED")).outcome());
        assertEquals("RELEASED", status(active));
        assertEquals(0, reserved(SITE, sku));
        assertEquals("ignored", direct(Harness.stateChanged(id(), active, "CANCELLED")).outcome());
        assertEquals(0, reserved(SITE, sku), "a repeat returns nothing more");

        String committed = newOrderId();
        direct(Harness.placed(id(), committed, SITE, sku, 2));
        direct(Harness.requested(id(), committed, Harness.reservationId(committed), SITE, sku, 2));
        assertEquals("reconciliation", direct(Harness.stateChanged(id(), committed, "CANCELLED")).outcome());
        assertEquals("COMMITTED", status(committed));
        assertEquals("ignored", direct(Harness.stateChanged(id(), newOrderId(), "CANCELLED")).outcome());
    }

    @Test
    void unknownSkuAndUnavailableSiteHaveStableReasons() {
        String sku = newSku("site_closed", 5);
        String closed = newOrderId();
        String unknownSite = newOrderId();
        String unknownSku = newOrderId();
        String scarce = newOrderId();
        send(ORDER_TOPIC, closed, Harness.placed(id(), closed, "site_closed", sku, 1));
        send(ORDER_TOPIC, unknownSite, Harness.placed(id(), unknownSite, "site_nowhere", "SKU-42", 1));
        send(ORDER_TOPIC, unknownSku, Harness.placed(id(), unknownSku, SITE, "SKU-NONE", 1));
        send(ORDER_TOPIC, scarce, Harness.placed(id(), scarce, SITE, newSku(SITE, 1), 2));
        for (String[] expected : new String[][] {{closed, "SITE_UNAVAILABLE"}, {unknownSite, "SITE_UNAVAILABLE"},
                {unknownSku, "UNKNOWN_SKU"}, {scarce, "INSUFFICIENT_STOCK"}}) {
            Harness.await("rejection", () -> rejected(expected[0]) == 1);
            assertEquals(expected[1], Harness.text("SELECT payload -> 'payload' ->> 'reasonCode' FROM outbox WHERE aggregate_id = ?", expected[0]));
        }
        assertEquals(0, reserved("site_closed", sku));
    }

    @Test
    void theReadApiShowsSiteSpecificAvailability() {
        String sku = newSku("site_nyc", 10);
        Harness.exec("INSERT INTO inventory_stock (site_id, sku, on_hand, updated_at) VALUES ('site_bos', '" + sku + "', 3, now())");
        String orderId = newOrderId();
        direct(Harness.placed(id(), orderId, "site_nyc", sku, 4));
        given().queryParam("siteId", "site_nyc").queryParam("sku", sku).when().get("/api/v1/inventory").then().statusCode(200)
                .body("size()", is(1)).body("[0].onHand", is(10)).body("[0].reserved", is(4)).body("[0].available", is(6));
        given().queryParam("siteId", "site_bos").queryParam("sku", sku).when().get("/api/v1/inventory").then().statusCode(200)
                .body("size()", is(1)).body("[0].available", is(3));
        given().queryParam("sku", sku).when().get("/api/v1/inventory").then().statusCode(200).body("size()", is(2));
        given().queryParam("siteId", "bad").when().get("/api/v1/inventory").then().statusCode(400).contentType(startsWith("application/problem+json"));
        given().queryParam("limit", "0").when().get("/api/v1/inventory").then().statusCode(400);
        given().queryParam("limit", "x").when().get("/api/v1/inventory").then().statusCode(400);
        given().queryParam("offset", "-1").when().get("/api/v1/inventory").then().statusCode(400);
        given().queryParam("siteId", "site_none").when().get("/api/v1/inventory").then().statusCode(200).body("size()", is(0));
        String id = Harness.reservationId(orderId);
        given().when().get("/api/v1/reservations/" + id).then().statusCode(200)
                .body("orderId", is(orderId)).body("status", is("ACTIVE")).body("quantity", is(4));
        given().when().get("/api/v1/reservations/res_missing").then().statusCode(404).contentType(startsWith("application/problem+json"));
        given().when().get("/api/v1/reservations/not-valid").then().statusCode(400);
    }

    @Test
    void theSeedFixtureIsLoaded() {
        assertEquals(3, count("SELECT count(*) FROM inventory_site"));
        assertEquals(100, onHand("site_nyc", "SKU-42"));
        assertEquals(1, count("SELECT count(*) FROM stock_movement WHERE site_id = 'site_nyc' AND sku = 'SKU-42' AND kind = 'SEED' AND delta = 100"));
    }

    @Test
    void restockIsAuthenticatedIdempotentAndAudited() {
        String sku = newSku(SITE, 1);
        String path = "/internal/v1/stock/" + SITE + "/" + sku + "/restock";
        String key = "key-" + sku;
        String body = "{\"quantity\":5,\"reason\":\"Cycle count\"}";
        given().contentType("application/json").header("Idempotency-Key", key).body(body).when().post(path).then().statusCode(401);
        given().contentType("application/json").header("Idempotency-Key", key).header("Authorization", "Bearer " + TestTokens.viewer())
                .body(body).when().post(path).then().statusCode(403);
        assertEquals(1, onHand(SITE, sku), "rejected calls change nothing");
        String token = TestTokens.admin("subject-operator");
        for (int i = 0; i < 2; i++) {
            given().contentType("application/json").header("Idempotency-Key", key).header("Authorization", "Bearer " + token)
                    .body(body).when().post(path).then().statusCode(200).body("onHand", is(6)).body("available", is(6));
        }
        assertEquals(6, onHand(SITE, sku), "the repeat changed nothing");
        given().contentType("application/json").header("Idempotency-Key", key).header("Authorization", "Bearer " + token)
                .body("{\"quantity\":6,\"reason\":\"Cycle count\"}").when().post(path).then().statusCode(409);
        assertEquals("subject-operator", Harness.text("SELECT actor FROM stock_movement WHERE sku = ? AND kind = 'RESTOCK'", sku));
        assertEquals("Cycle count", Harness.text("SELECT reason FROM stock_movement WHERE sku = ? AND kind = 'RESTOCK'", sku));
        given().contentType("application/json").header("Idempotency-Key", "other").header("Authorization", "Bearer " + token)
                .body(body).when().post("/internal/v1/stock/site_nyc/SKU-NONE/restock").then().statusCode(404);
    }

    @Test
    void releaseIsAuthenticatedRepeatableAndAudited() {
        String sku = newSku(SITE, 5);
        String orderId = newOrderId();
        direct(Harness.placed(id(), orderId, SITE, sku, 2));
        direct(Harness.requested(id(), orderId, Harness.reservationId(orderId), SITE, sku, 2));
        String path = "/internal/v1/reservations/" + Harness.reservationId(orderId) + "/release";
        given().contentType("application/json").body("{\"reason\":\"x\"}").when().post(path).then().statusCode(401);
        assertEquals("COMMITTED", status(orderId));
        String token = TestTokens.admin("subject-operator");
        for (int i = 0; i < 2; i++) {
            given().contentType("application/json").header("Authorization", "Bearer " + token).body("{\"reason\":\"Reconciled\"}")
                    .when().post(path).then().statusCode(200).body("status", is("RELEASED"));
        }
        assertEquals(0, reserved(SITE, sku), "returned once");
        assertEquals(1, count("SELECT count(*) FROM reservation_history WHERE reservation_id = ? AND to_status = 'RELEASED' AND actor = 'subject-operator' AND reason = 'Reconciled'", Harness.reservationId(orderId)));

        String shipped = newOrderId();
        direct(Harness.placed(id(), shipped, SITE, sku, 1));
        direct(Harness.shipped(id(), shipped, "shp_r"));
        given().contentType("application/json").header("Authorization", "Bearer " + token).body("{\"reason\":\"x\"}")
                .when().post("/internal/v1/reservations/" + Harness.reservationId(shipped) + "/release")
                .then().statusCode(409).contentType(startsWith("application/problem+json"));
        assertEquals("CONSUMED", status(shipped));
        given().contentType("application/json").header("Authorization", "Bearer " + token).body("{\"reason\":\"x\"}")
                .when().post("/internal/v1/reservations/res_missing/release").then().statusCode(404);
    }

    @Test
    void metricsExistAndStayConsistent() {
        String sku = newSku(SITE, 5);
        String orderId = newOrderId();
        send(ORDER_TOPIC, orderId, Harness.placed(id(), orderId, SITE, sku, 2));
        Harness.await("hold", () -> "ACTIVE".equals(status(orderId)));
        send(ORDER_TOPIC, newOrderId(), "{\"poison\":true}");
        Harness.await("reserved metric", () -> given().when().get("/q/metrics").asString().contains("box1_inventory_reserved_units_total{site=\"site_nyc\"}"));
        String body = given().when().get("/q/metrics").asString();
        for (String name : new String[] {"box1_inventory_reserved_units_total", "box1_events_consumed_total", "box1_event_age_seconds_count",
                "box1_inventory_dlq_depth", "box1_outbox_pending", "box1_outbox_oldest_pending_age_seconds",
                "box1_inventory_reservations_active", "box1_inventory_reconciliation_open", "box1_inventory_reserved_drift",
                "box1_inventory_committed_oldest_age_seconds"}) {
            assertTrue(body.contains(name), name);
        }
        Harness.await("dead-letter depth", () -> gauge("box1_inventory_dlq_depth") > 0);
        Harness.await("no drift", () -> gauge("box1_inventory_reserved_drift") == 0);
    }

    private static double gauge(String name) {
        return given().when().get("/q/metrics").asString().lines().filter(l -> l.startsWith(name + " ") || l.startsWith(name + "{"))
                .mapToDouble(l -> Double.parseDouble(l.substring(l.lastIndexOf(' ') + 1))).findFirst().orElse(-1);
    }

    @Test
    void customerDataIsNotKeptOrRepublished() {
        assertEquals(0, count("SELECT count(*) FROM information_schema.columns WHERE table_schema = 'public' AND column_name ILIKE '%customer%'"));
        String sku = newSku(SITE, 5);
        String orderId = newOrderId();
        direct(Harness.placed(id(), orderId, SITE, sku, 1));
        assertEquals(0, count("SELECT count(*) FROM outbox WHERE aggregate_id = ? AND payload::text LIKE '%cust_%'", orderId));
    }
}
