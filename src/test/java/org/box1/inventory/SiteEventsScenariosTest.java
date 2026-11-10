package org.box1.inventory;

import static io.restassured.RestAssured.given;
import static org.box1.inventory.Harness.FULFILLMENT_TOPIC;
import static org.box1.inventory.Harness.ORDER_TOPIC;
import static org.box1.inventory.Harness.count;
import static org.box1.inventory.Harness.id;
import static org.box1.inventory.Harness.newOrderId;
import static org.box1.inventory.Harness.newSku;
import static org.box1.inventory.Harness.onHand;
import static org.box1.inventory.Harness.reserved;
import static org.box1.inventory.Harness.send;
import static org.box1.inventory.Harness.sendSite;
import static org.box1.inventory.Harness.status;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.box1.inventory.messaging.EventParser;
import org.box1.inventory.messaging.SiteEventParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Site events (box1-docs#129) against real PostgreSQL and Kafka, in the same application as the order scenarios. */
@QuarkusTest
@ContainersTag
@QuarkusTestResource(TestTokens.class)
class SiteEventsScenariosTest {
    private static final String SITE = "site_nyc";
    private static final Set<String> ORDER_TYPES = orderTypes();

    @Inject
    SiteEventService sites;

    @Inject
    SiteEventParser siteParser;

    @Inject
    ReservationService reservations;

    @Inject
    EventParser parser;

    @Inject
    ExpiryScheduler scheduler;

    @Inject
    TestClock clock;

    private static Set<String> orderTypes() {
        Set<String> types = new HashSet<>(EventParser.ORDER_TOPIC_TYPES);
        types.addAll(EventParser.FULFILLMENT_TOPIC_TYPES);
        return types;
    }

    @AfterEach
    void cleanUp() {
        clock.reset();
    }

    private String site(String value) {
        return sites.apply(siteParser.parse(value).orElseThrow());
    }

    private ReservationService.Applied order(String value) {
        return reservations.apply(parser.parse(value, ORDER_TYPES));
    }

    /** Place an order directly and return its reservationId. */
    private String placed(String orderId, String sku, int quantity) {
        order(Harness.placed(id(), orderId, SITE, sku, quantity));
        return Harness.reservationId(orderId);
    }

    private static String pickId() {
        return "pick_" + UUID.randomUUID().toString().replace("-", "");
    }

    private static String countId() {
        return "cnt_" + UUID.randomUUID().toString().replace("-", "");
    }

    private static String holdId() {
        return "hold_" + UUID.randomUUID().toString().replace("-", "");
    }

    private static long items(String kind, String eventId) {
        return count("SELECT count(*) FROM reconciliation_item WHERE kind = ? AND event_id = ?::uuid", kind, eventId);
    }

    /** Everything a site event can change for one SKU, as text, to compare before and after a replay. */
    private static String state(String sku) {
        return String.join("|", Harness.text("SELECT on_hand || ',' || reserved FROM inventory_stock WHERE site_id = ? AND sku = ?", SITE, sku),
                Harness.text("SELECT count(*) || ',' || COALESCE(sum(delta), 0) FROM stock_movement WHERE site_id = ? AND sku = ?", SITE, sku),
                Harness.text("SELECT count(*) || '' FROM site_count WHERE sku = ?", sku),
                Harness.text("SELECT count(*) || '' FROM site_pick WHERE sku = ?", sku),
                Harness.text("SELECT count(*) || '' FROM site_hold WHERE sku = ?", sku),
                Harness.text("SELECT count(*) || '' FROM reconciliation_item WHERE sku = ? OR order_id IN"
                        + " (SELECT order_id FROM reservations WHERE sku = ?)", sku, sku),
                Harness.text("SELECT string_agg(status, ',' ORDER BY order_id) FROM reservations WHERE sku = ?", sku));
    }

    private static List<ConsumerRecord<String, String>> siteDlq(String key) {
        List<ConsumerRecord<String, String>> matched = new ArrayList<>();
        for (ConsumerRecord<String, String> record : Harness.read(Harness.SITE_DLQ_TOPIC, Duration.ofSeconds(3))) {
            if (key.equals(record.key())) {
                matched.add(record);
            }
        }
        return matched;
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        return new String(record.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }

    @Test
    void aPickChangesNeitherOnHandNorReserved() {
        String sku = newSku(SITE, 10);
        String orderId = newOrderId();
        String reservationId = placed(orderId, sku, 3);
        String eventId = id();
        assertEquals("pick_picked", site(Harness.pick(eventId, SITE, pickId(), orderId, reservationId, sku, 3)));
        assertEquals(10, onHand(SITE, sku));
        assertEquals(3, reserved(SITE, sku));
        assertEquals("ACTIVE", status(orderId), "a pick never changes the reservation");
        assertEquals(0, count("SELECT count(*) FROM reconciliation_item WHERE event_id = ?::uuid", eventId));
        assertEquals("ACTIVE", Harness.text("SELECT reservation_status FROM site_pick WHERE event_id = ?::uuid", eventId));

        String committed = newOrderId();
        String committedId = placed(committed, sku, 1);
        order(Harness.requested(id(), committed, committedId, SITE, sku, 1));
        assertEquals("pick_picked", site(Harness.pick(id(), SITE, pickId(), committed, committedId, sku, 1)));
        assertEquals("COMMITTED", status(committed));
    }

    @Test
    void aPickAfterTheShipmentIsRecordedAsSuch() {
        String sku = newSku(SITE, 10);
        String orderId = newOrderId();
        String reservationId = placed(orderId, sku, 2);
        order(Harness.shipped(id(), orderId, "shp_" + orderId.substring(4)));
        assertEquals(8, onHand(SITE, sku));
        assertEquals("pick_picked_after_shipment", site(Harness.pick(id(), SITE, pickId(), orderId, reservationId, sku, 2)));
        assertEquals(8, onHand(SITE, sku), "the shipment already took the units; the pick takes nothing");
        assertEquals(0, reserved(SITE, sku));
    }

    @Test
    void aPickOfAnExpiredOrReleasedHoldOpensPickNotHeld() {
        String sku = newSku(SITE, 10);
        String expired = newOrderId();
        String expiredId = placed(expired, sku, 2);
        clock.advance(Duration.ofMinutes(16));
        scheduler.expireDue();
        Harness.await("expired", () -> "EXPIRED".equals(status(expired)));
        clock.reset();
        String eventId = id();
        assertEquals("pick_not_held", site(Harness.pick(eventId, SITE, pickId(), expired, expiredId, sku, 2)));
        assertEquals(1, items("PICK_NOT_HELD", eventId));
        assertEquals(expired, Harness.text("SELECT order_id FROM reconciliation_item WHERE event_id = ?::uuid", eventId));
        assertEquals("EXPIRED", Harness.text("SELECT detail->>'reservationStatus' FROM reconciliation_item WHERE event_id = ?::uuid", eventId));

        String released = newOrderId();
        String releasedId = placed(released, sku, 1);
        reservations.release(releasedId, "subject-test", "test");
        String other = id();
        assertEquals("pick_not_held", site(Harness.pick(other, SITE, pickId(), released, releasedId, sku, 1)));
        assertEquals(1, items("PICK_NOT_HELD", other));
        assertEquals(10, onHand(SITE, sku));
        assertEquals(0, reserved(SITE, sku));
        assertEquals("EXPIRED", status(expired), "the reservation stays as it was");
        assertEquals("RELEASED", status(released));
    }

    @Test
    void anUnknownOrDifferentPickOpensPickMismatch() {
        String sku = newSku(SITE, 10);
        String orderId = newOrderId();
        String reservationId = placed(orderId, sku, 2);
        String unknown = id();
        assertEquals("pick_mismatch", site(Harness.pick(unknown, SITE, pickId(), orderId, "res_doesnotexist", sku, 2)));
        assertEquals(1, items("PICK_MISMATCH", unknown));
        assertEquals("UNKNOWN", Harness.text("SELECT detail->>'reservation' FROM reconciliation_item WHERE event_id = ?::uuid", unknown));
        String quantity = id();
        assertEquals("pick_mismatch", site(Harness.pick(quantity, SITE, pickId(), orderId, reservationId, sku, 3)));
        assertEquals("[\"quantity\"]", Harness.text("SELECT detail->>'differs' FROM reconciliation_item WHERE event_id = ?::uuid", quantity));
        String otherSite = id();
        assertEquals("pick_mismatch", site(Harness.pick(otherSite, "site_bos", pickId(), orderId, reservationId, sku, 2)));
        assertEquals("[\"siteId\"]", Harness.text("SELECT detail->>'differs' FROM reconciliation_item WHERE event_id = ?::uuid", otherSite));
        assertEquals("site_bos", Harness.text("SELECT site_id FROM reconciliation_item WHERE event_id = ?::uuid", otherSite));
        assertEquals(10, onHand(SITE, sku));
        assertEquals(2, reserved(SITE, sku));
        assertEquals("ACTIVE", status(orderId));
    }

    @Test
    void aRepeatedPickIdOrEventIdIsADuplicate() {
        String sku = newSku(SITE, 10);
        String orderId = newOrderId();
        String reservationId = placed(orderId, sku, 2);
        String pickId = pickId();
        String eventId = id();
        String value = Harness.pick(eventId, SITE, pickId, orderId, "res_unknownforthistest", sku, 2);
        assertEquals("pick_mismatch", site(value));
        String before = state(sku);
        assertEquals("duplicate", site(value));
        assertEquals("duplicate_pick", site(Harness.pick(id(), SITE, pickId, orderId, reservationId, sku, 2)), "same pickId, new eventId");
        assertEquals(before, state(sku));
        assertEquals(1, count("SELECT count(*) FROM site_pick WHERE pick_id = ?", pickId));
    }

    @Test
    void aCountAppliesTheRuleWithAZeroOrNonZeroDelta() {
        String sku = newSku(SITE, 20);
        placed(newOrderId(), sku, 3);
        Instant t = Instant.now().truncatedTo(ChronoUnit.MICROS);
        String first = countId();
        assertEquals("count_applied", site(Harness.count(id(), SITE, first, sku, 17, t)));
        assertEquals(17, onHand(SITE, sku));
        assertEquals(3, reserved(SITE, sku), "a count never changes reserved");
        assertEquals(-3, count("SELECT delta FROM stock_movement WHERE kind = 'COUNT' AND reference = ?", first));
        assertEquals("site-count:" + first, Harness.text("SELECT idempotency_key FROM stock_movement WHERE reference = ?", first));
        assertEquals(1, count("SELECT count(*) FROM stock_movement WHERE reference = ? AND business_at = ?::timestamptz"
                + " AND occurred_at = business_at", first, t.toString()), "the movement's time is countedAt");
        String agrees = countId();
        assertEquals("count_applied", site(Harness.count(id(), SITE, agrees, sku, 17, t.plusSeconds(1))));
        assertEquals(17, onHand(SITE, sku));
        assertEquals(0, count("SELECT delta FROM stock_movement WHERE kind = 'COUNT' AND reference = ?", agrees), "a zero delta is still recorded");
        assertEquals("17|0|3|17|17", Harness.text("SELECT expected || '|' || later_movements || '|' || reserved || '|' || on_hand_before"
                + " || '|' || on_hand_after FROM site_count WHERE count_id = ?", agrees));
    }

    /** shipment.confirmed.v1 takes on_hand and reserved down together, and its movement's business time is shippedAt. */
    @Test
    void shipmentsCountByShippedAtNotByProcessingTime() {
        String sku = newSku(SITE, 10);
        String orderId = newOrderId();
        placed(orderId, sku, 4);
        Instant shippedAt = Instant.now().minus(Duration.ofHours(2)).truncatedTo(ChronoUnit.MICROS);
        order(Harness.shippedAt(id(), orderId, "shp_" + orderId.substring(4), shippedAt));
        assertEquals(6, onHand(SITE, sku), "on_hand falls by the shipped quantity");
        assertEquals(0, reserved(SITE, sku), "reserved falls by the same quantity");
        assertEquals(1, count("SELECT count(*) FROM stock_movement WHERE site_id = ? AND sku = ? AND kind = 'SHIPMENT' AND delta = -4"
                + " AND business_at = ?::timestamptz AND occurred_at > business_at", SITE, sku, shippedAt.toString()));
        // A count taken after the shipment left but before it was processed: the shipment is not after the count.
        assertEquals("count_applied", site(Harness.count(id(), SITE, countId(), sku, 6, shippedAt.plus(Duration.ofHours(1)))));
        assertEquals(6, onHand(SITE, sku));
        // A count taken before the shipment left: the shipment's -4 is after it, so 10 counted means 6 now.
        String before = countId();
        String other = newSku(SITE, 10);
        String otherOrder = newOrderId();
        placed(otherOrder, other, 4);
        Instant later = Instant.now().minus(Duration.ofMinutes(30)).truncatedTo(ChronoUnit.MICROS);
        order(Harness.shippedAt(id(), otherOrder, "shp_" + otherOrder.substring(4), later));
        assertEquals("count_applied", site(Harness.count(id(), SITE, before, other, 10, later.minus(Duration.ofMinutes(5)))));
        assertEquals(6, onHand(SITE, other));
        assertEquals("6|-4|0|6|6", Harness.text("SELECT expected || '|' || later_movements || '|' || reserved || '|' || on_hand_before"
                + " || '|' || on_hand_after FROM site_count WHERE count_id = ?", before));
    }

    @Test
    void aLaterAppliedCountSupersedesAnEarlierOne() {
        String sku = newSku(SITE, 20);
        Instant t = Instant.now().truncatedTo(ChronoUnit.MICROS);
        assertEquals("count_applied", site(Harness.count(id(), SITE, countId(), sku, 15, t)));
        assertEquals("count_superseded", site(Harness.count(id(), SITE, countId(), sku, 5, t.minusSeconds(60))));
        assertEquals(15, onHand(SITE, sku));
        // A tie on countedAt goes to the greater countId.
        assertEquals("count_applied", site(Harness.count(id(), SITE, "cnt_tie_b", sku, 12, t.plusSeconds(10))));
        assertEquals("count_superseded", site(Harness.count(id(), SITE, "cnt_tie_a", sku, 11, t.plusSeconds(10))));
        assertEquals(12, onHand(SITE, sku));
        assertEquals(1, count("SELECT count(*) FROM site_count WHERE count_id = 'cnt_tie_a' AND outcome = 'SUPERSEDED'"));
        assertEquals(0, count("SELECT count(*) FROM stock_movement WHERE reference = 'cnt_tie_a'"), "a superseded count moves nothing");
    }

    /** The same counts, reservations and movements in two arrival orders end at the same on_hand. */
    @Test
    void twoCountsInEitherArrivalOrderGiveTheSameOnHand() {
        Instant base = Instant.now().minus(Duration.ofHours(1)).truncatedTo(ChronoUnit.MICROS);
        String ab = newSku(SITE, 30);
        String ba = newSku(SITE, 30);
        for (String sku : List.of(ab, ba)) {
            placed(newOrderId(), sku, 5);
            String shipped = newOrderId();
            placed(shipped, sku, 2);
            order(Harness.shippedAt(id(), shipped, "shp_" + shipped.substring(4), base.plusSeconds(30)));
        }
        Instant ta = base;
        Instant tb = base.plusSeconds(60);
        site(Harness.count(id(), SITE, countId() + "a", ab, 25, ta));
        site(Harness.count(id(), SITE, countId() + "b", ab, 21, tb));
        site(Harness.count(id(), SITE, countId() + "b", ba, 21, tb));
        site(Harness.count(id(), SITE, countId() + "a", ba, 25, ta));
        assertEquals(onHand(SITE, ab), onHand(SITE, ba));
        assertEquals(21, onHand(SITE, ab), "the latest count wins: 21 counted after the shipment");
        assertEquals(reserved(SITE, ab), reserved(SITE, ba));
    }

    @Test
    void aCountBelowReservedIsHeldAtReservedAndOpensCountConflict() {
        String sku = newSku(SITE, 5);
        placed(newOrderId(), sku, 4);
        String eventId = id();
        String countId = countId();
        assertEquals("count_conflict", site(Harness.count(eventId, SITE, countId, sku, 2, Instant.now())));
        assertEquals(4, onHand(SITE, sku), "held at reserved");
        assertEquals(4, reserved(SITE, sku), "no reservation is cancelled");
        assertEquals(1, items("COUNT_CONFLICT", eventId));
        assertNull(Harness.text("SELECT order_id FROM reconciliation_item WHERE event_id = ?::uuid", eventId));
        assertEquals("2|4|2|4", Harness.text("SELECT (detail->>'expected') || '|' || (detail->>'reserved') || '|' || (detail->>'shortfall')"
                + " || '|' || (detail->>'onHandAfter') FROM reconciliation_item WHERE event_id = ?::uuid", eventId));
        given().when().get("/api/v1/reconciliation-items?kind=COUNT_CONFLICT&siteId=" + SITE + "&status=OPEN").then().statusCode(200)
                .body("eventId", hasItem(eventId)).body("kind", everyItem(is("COUNT_CONFLICT")))
                .body("find { it.eventId == '" + eventId + "' }.detail.shortfall", is(2))
                .body("find { it.eventId == '" + eventId + "' }.sku", is(sku));
        given().when().get("/api/v1/site-events?type=COUNT&siteId=" + SITE).then().statusCode(200)
                .body("find { it.id == '" + countId + "' }.outcome", is("APPLIED"))
                .body("find { it.id == '" + countId + "' }.expected", is(2))
                .body("find { it.id == '" + countId + "' }.onHandAfter", is(4));
    }

    @Test
    void aNegativeExpectedCountIsHeldAtZero() {
        String sku = newSku(SITE, 5);
        String orderId = newOrderId();
        placed(orderId, sku, 3);
        Instant shippedAt = Instant.now().truncatedTo(ChronoUnit.MICROS);
        order(Harness.shippedAt(id(), orderId, "shp_" + orderId.substring(4), shippedAt));
        String eventId = id();
        assertEquals("count_conflict", site(Harness.count(eventId, SITE, countId(), sku, 1, shippedAt.minusSeconds(60))));
        assertEquals(0, onHand(SITE, sku));
        assertEquals("-2|0|2", Harness.text("SELECT (detail->>'expected') || '|' || (detail->>'reserved') || '|' || (detail->>'shortfall')"
                + " FROM reconciliation_item WHERE event_id = ?::uuid", eventId));
    }

    @Test
    void aCountOfUnknownStockChangesNothingAndOpensAnItem() {
        String sku = "SKU-unknown-" + UUID.randomUUID();
        String eventId = id();
        assertEquals("count_unknown_stock", site(Harness.count(eventId, SITE, countId(), sku, 3, Instant.now())));
        assertEquals(1, items("COUNT_UNKNOWN_STOCK", eventId));
        assertEquals(0, count("SELECT count(*) FROM inventory_stock WHERE sku = ?", sku), "no stock row is created");
        assertEquals(0, count("SELECT count(*) FROM stock_movement WHERE sku = ?", sku));
    }

    @Test
    void holdsAreRecordedAndChangeNoStock() {
        String sku = newSku(SITE, 5);
        String holdId = holdId();
        assertEquals("hold_placed", site(Harness.hold(id(), SITE, holdId, sku, 2, "PLACED")));
        assertEquals("duplicate_hold", site(Harness.hold(id(), SITE, holdId, sku, 2, "PLACED")));
        assertEquals("hold_released", site(Harness.hold(id(), SITE, holdId, sku, 2, "RELEASED")));
        assertEquals("duplicate_hold", site(Harness.hold(id(), SITE, holdId, sku, 2, "RELEASED")));
        assertEquals(2, count("SELECT count(*) FROM site_hold WHERE hold_id = ?", holdId));
        String unknown = id();
        assertEquals("hold_unknown", site(Harness.hold(unknown, SITE, holdId(), sku, 1, "RELEASED")));
        assertEquals(1, items("HOLD_UNKNOWN", unknown));
        assertEquals(5, onHand(SITE, sku));
        assertEquals(0, reserved(SITE, sku));
        given().when().get("/api/v1/site-events?type=HOLD&siteId=" + SITE).then().statusCode(200)
                .body("findAll { it.id == '" + holdId + "' }.action", org.hamcrest.Matchers.containsInAnyOrder("PLACED", "RELEASED"));
    }

    /** Picks, counts and holds through Kafka, then the same records replayed and re-sent under new eventIds. */
    @Test
    void aReplayedBacklogLeavesEveryRowUnchanged() {
        String sku = newSku(SITE, 12);
        String orderId = newOrderId();
        String reservationId = placed(orderId, sku, 2);
        Instant t = Instant.now().truncatedTo(ChronoUnit.MICROS);
        String pickId = pickId();
        String countId = countId();
        String holdId = holdId();
        List<String> backlog = List.of(
                Harness.pick(id(), SITE, pickId, orderId, reservationId, sku, 2),
                Harness.count(id(), SITE, countId, sku, 11, t),
                Harness.hold(id(), SITE, holdId, sku, 1, "PLACED"),
                Harness.pick(id(), SITE, pickId(), orderId, "res_missing", sku, 2));
        for (String value : backlog) {
            sendSite(SITE, value);
        }
        for (String value : backlog) {
            Harness.awaitProcessed(Harness.parse(value).get("eventId").asText());
        }
        assertEquals(11, onHand(SITE, sku));
        String before = state(sku);
        for (String value : backlog) {
            sendSite(SITE, value);
        }
        // The same identities under new eventIds, as a site outbox rebuilt from scratch would send them.
        String marker = id();
        sendSite(SITE, Harness.pick(id(), SITE, pickId, orderId, reservationId, sku, 2));
        sendSite(SITE, Harness.count(id(), SITE, countId, sku, 11, t));
        sendSite(SITE, Harness.hold(marker, SITE, holdId, sku, 1, "PLACED"));
        Harness.awaitProcessed(marker);
        assertEquals(before, state(sku), "replay changed nothing");
        assertEquals(1, count("SELECT count(*) FROM stock_movement WHERE reference = ? AND kind = 'COUNT'", countId));
    }

    @Test
    void poisonGoesToTheSiteDeadLetterTopicAndOtherTypesAreIgnored() {
        String key = "site_poison" + UUID.randomUUID().toString().substring(0, 8);
        sendSite(key, "{not json");
        Harness.await("dead letter", () -> !siteDlq(key).isEmpty());
        ConsumerRecord<String, String> record = siteDlq(key).get(0);
        assertEquals("{not json", record.value(), "unchanged");
        assertEquals("Record value is not valid JSON", header(record, "dead-letter-reason"));
        assertEquals(Harness.SITE_TOPIC, header(record, "dead-letter-topic"));

        String sku = newSku(SITE, 5);
        String envelope = "site_envelope" + UUID.randomUUID().toString().substring(0, 8);
        sendSite(envelope, Harness.count(id(), SITE, countId(), sku, 3, Instant.now()).replace("\"aggregateId\":\"" + SITE + "\"",
                "\"aggregateId\":\"site_bos\""));
        String payload = "site_payload" + UUID.randomUUID().toString().substring(0, 8);
        sendSite(payload, Harness.count(id(), SITE, countId(), sku, 3, Instant.now()).replace("\"countedQuantity\":3", "\"countedQuantity\":-3"));
        Harness.await("envelope dead letter", () -> !siteDlq(envelope).isEmpty());
        Harness.await("payload dead letter", () -> !siteDlq(payload).isEmpty());
        assertEquals("aggregateId does not match siteId", header(siteDlq(envelope).get(0), "dead-letter-reason"));
        assertEquals("Invalid countedQuantity", header(siteDlq(payload).get(0), "dead-letter-reason"));
        assertEquals(5, onHand(SITE, sku));

        String other = "site_other" + UUID.randomUUID().toString().substring(0, 8);
        String otherType = Harness.hold(id(), SITE, holdId(), sku, 1, "PLACED").replace("site.stock-held.v1", "site.reading-batched.v1");
        sendSite(other, otherType);
        String marker = id();
        sendSite(other, Harness.hold(marker, SITE, holdId(), sku, 1, "PLACED"));
        Harness.awaitProcessed(marker);
        assertTrue(siteDlq(other).isEmpty(), "another event type is acknowledged, not dead-lettered");
        assertTrue(Harness.read(Harness.DLQ_TOPIC, Duration.ofSeconds(2)).stream().noneMatch(r -> key.equals(r.key())),
                "nothing from the site topic reaches the inventory dead-letter topic");
    }

    @Test
    void anUnavailableDatabaseHoldsASiteEventWithoutDeadLettering() {
        String sku = newSku(SITE, 5);
        String eventId = id();
        Harness.exec("CREATE OR REPLACE FUNCTION site_unavailable() RETURNS trigger AS $$ BEGIN"
                + " IF NEW.event_id = '" + eventId + "' THEN RAISE EXCEPTION 'injected' USING ERRCODE = '57P03'; END IF; RETURN NEW; END; $$ LANGUAGE plpgsql");
        Harness.exec("CREATE TRIGGER site_unavailable_trigger BEFORE INSERT ON processed_event FOR EACH ROW EXECUTE FUNCTION site_unavailable()");
        try {
            double before = metric("box1_inventory_consumer_held_for_database_total", "");
            sendSite(SITE, Harness.count(eventId, SITE, countId(), sku, 4, Instant.now()));
            // The test bound is 5 attempts, so 8 held retries prove attempts are not being used up.
            Harness.await("more held retries than the attempt bound",
                    () -> metric("box1_inventory_consumer_held_for_database_total", "") >= before + 8);
            assertTrue(siteDlq(SITE).stream().noneMatch(r -> r.value().contains(eventId)), "nothing dead-lettered");
            assertEquals(5, onHand(SITE, sku));
        } finally {
            Harness.exec("DROP TRIGGER site_unavailable_trigger ON processed_event");
        }
        Harness.awaitProcessed(eventId);
        assertEquals(4, onHand(SITE, sku));
    }

    private static double metric(String name, String label) {
        return given().when().get("/q/metrics").asString().lines()
                .filter(l -> (l.startsWith(name + " ") || l.startsWith(name + "{")) && l.contains(label))
                .mapToDouble(l -> Double.parseDouble(l.substring(l.lastIndexOf(' ') + 1))).sum();
    }

    /** The order → ERP → ship flow, entirely through Kafka, with site events for the same order and SKU interleaved. */
    @Test
    void anOrderStillReservesAndShipsWithSiteEventsInterleaved() {
        String sku = newSku(SITE, 9);
        String orderId = newOrderId();
        send(ORDER_TOPIC, orderId, Harness.placed(id(), orderId, SITE, sku, 2));
        Harness.await("reserved", () -> "ACTIVE".equals(status(orderId)));
        String reservationId = Harness.reservationId(orderId);
        sendSite(SITE, Harness.pick(id(), SITE, pickId(), orderId, reservationId, sku, 2));
        sendSite(SITE, Harness.hold(id(), SITE, holdId(), sku, 1, "PLACED"));
        send(ORDER_TOPIC, orderId, Harness.requested(id(), orderId, reservationId, SITE, sku, 2));
        Harness.await("committed", () -> "COMMITTED".equals(status(orderId)));
        String countEvent = id();
        sendSite(SITE, Harness.count(countEvent, SITE, countId(), sku, 9, Instant.now()));
        String shipEvent = id();
        send(FULFILLMENT_TOPIC, orderId, Harness.shipped(shipEvent, orderId, "shp_" + orderId.substring(4)));
        Harness.awaitProcessed(countEvent);
        Harness.awaitProcessed(shipEvent);
        assertEquals("CONSUMED", status(orderId));
        assertEquals(7, onHand(SITE, sku), "nine counted with two picked and not yet shipped; the shipment took two");
        assertEquals(0, reserved(SITE, sku));
        assertEquals(1, Harness.outbox(orderId, "inventory.reserved.v1"));
        assertEquals(0, Harness.outbox(orderId, "inventory.reservation-expired.v1"));
        assertEquals(0, count("SELECT count(*) FROM reconciliation_item WHERE order_id = ?", orderId), "no reconciliation for the order");
        assertEquals("ACTIVE,COMMITTED,CONSUMED", Harness.text("SELECT string_agg(to_status, ',' ORDER BY seq) FROM reservation_history"
                + " WHERE reservation_id = ?", reservationId));
        assertEquals(1, count("SELECT count(*) FROM stock_movement WHERE site_id = ? AND sku = ? AND kind = 'SHIPMENT' AND delta = -2", SITE, sku));
    }
}
