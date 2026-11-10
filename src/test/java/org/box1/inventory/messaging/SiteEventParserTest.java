package org.box1.inventory.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.box1.inventory.domain.PoisonEventException;
import org.box1.inventory.domain.SiteEvent;
import org.junit.jupiter.api.Test;

/** What the site consumer accepts, ignores and dead-letters. Runs in CI. */
class SiteEventParserTest {
    private static final SiteEventParser PARSER = new SiteEventParser(new EventParser(new ObjectMapper()));
    private static final String PICK = """
            {"eventId":"61616161-6161-4161-8161-616161616161","eventType":"site.pick-confirmed.v1","schemaVersion":1,
             "aggregateId":"site_ds9","occurredAt":"2026-11-10T12:05:00Z","correlationId":"ord_2001",
             "causationId":"22222222-2222-4222-8222-222222222222",
             "traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
             "payload":{"siteId":"site_ds9","pickId":"pick_6f1c2a","orderId":"ord_2001","reservationId":"res_2001",
                        "sku":"SKU-42","quantity":2,"pickedAt":"2026-11-10T12:04:58Z"}}""";
    private static final String COUNT = """
            {"eventId":"62626262-6262-4262-8262-626262626262","eventType":"site.stock-adjusted.v1","schemaVersion":1,
             "aggregateId":"site_ds9","occurredAt":"2026-11-10T12:10:00Z","correlationId":"cnt_9d3e41","causationId":"site-ops:count",
             "payload":{"siteId":"site_ds9","countId":"cnt_9d3e41","sku":"SKU-42","countedQuantity":17,
                        "countedAt":"2026-11-10T12:09:30Z","reason":"CYCLE_COUNT"}}""";
    private static final String HOLD = """
            {"eventId":"63636363-6363-4363-8363-636363636363","eventType":"site.stock-held.v1","schemaVersion":1,
             "aggregateId":"site_spacedock","occurredAt":"2026-11-10T12:15:00Z","correlationId":"hold_51ab07","causationId":"site-ops:hold",
             "payload":{"siteId":"site_spacedock","holdId":"hold_51ab07","sku":"SKU-7","quantity":1,"action":"PLACED",
                        "reasonCode":"DAMAGED","changedAt":"2026-11-10T12:14:40Z"}}""";

    @Test
    void theThreeSiteEventsParse() {
        SiteEvent pick = PARSER.parse(PICK).orElseThrow();
        assertEquals("site_ds9", pick.siteId());
        assertEquals(new SiteEvent.Pick("pick_6f1c2a", "ord_2001", "res_2001", "SKU-42", 2,
                java.time.Instant.parse("2026-11-10T12:04:58Z")), pick.payload());
        SiteEvent.Count count = (SiteEvent.Count) PARSER.parse(COUNT).orElseThrow().payload();
        assertEquals(17, count.countedQuantity());
        SiteEvent.Hold hold = (SiteEvent.Hold) PARSER.parse(HOLD).orElseThrow().payload();
        assertEquals("PLACED", hold.action());
        assertEquals("pick_6f1c2a", ((SiteEvent.Pick) PARSER.parse(PICK.replace("\"pickId\"", "\"futureField\":1,\"pickId\""))
                .orElseThrow().payload()).pickId(), "an unknown payload field is an additive change");
    }

    @Test
    void anotherEventTypeIsIgnoredNotPoison() {
        assertTrue(PARSER.parse(PICK.replace("site.pick-confirmed.v1", "site.reading-batched.v1")).isEmpty());
        assertTrue(PARSER.parse(PICK.replace("site.pick-confirmed.v1", "order.placed.v1")).isEmpty());
        assertTrue(PARSER.parse(PICK.replace("site.pick-confirmed.v1", "site.pick-confirmed.v2")
                .replace("\"schemaVersion\":1", "\"schemaVersion\":2")).isEmpty(), "a future version is not this consumer's");
    }

    @Test
    void invalidRecordsArePoison() {
        String[] bad = {
            null, "", "not json", "[]", "{}", "{\"eventType\":7}",
            PICK.replace("\"schemaVersion\":1", "\"schemaVersion\":2"),
            PICK.replace("61616161-6161-4161-8161-616161616161", "not-a-uuid"),
            PICK.replace("\"aggregateId\":\"site_ds9\"", "\"aggregateId\":\"site_spacedock\""),
            PICK.replace("\"occurredAt\":\"2026-11-10T12:05:00Z\"", "\"occurredAt\":\"later\""),
            PICK.replace("\"correlationId\":\"ord_2001\",", ""),
            PICK.replace("00-4bf9", "99-4bf9"),
            PICK.replace("\"pickId\":\"pick_6f1c2a\"", "\"pickId\":\"6f1c2a\""),
            PICK.replace("\"orderId\":\"ord_2001\"", "\"orderId\":\"2001\""),
            PICK.replace("\"reservationId\":\"res_2001\",", ""),
            PICK.replace("\"quantity\":2", "\"quantity\":0"),
            PICK.replace("\"quantity\":2", "\"quantity\":\"2\""),
            PICK.replace("\"pickedAt\":\"2026-11-10T12:04:58Z\"", "\"pickedAt\":\"noon\""),
            PICK.replace("\"siteId\":\"site_ds9\"", "\"siteId\":\"ds9\""),
            COUNT.replace("\"countedQuantity\":17", "\"countedQuantity\":-1"),
            COUNT.replace("\"countedQuantity\":17", "\"countedQuantity\":1000001"),
            COUNT.replace("\"countId\":\"cnt_9d3e41\"", "\"countId\":\"cnt_\""),
            COUNT.replace("\"reason\":\"CYCLE_COUNT\"", "\"reason\":\"GUESS\""),
            COUNT.replace("\"sku\":\"SKU-42\",", ""),
            HOLD.replace("\"action\":\"PLACED\"", "\"action\":\"MOVED\""),
            HOLD.replace("\"reasonCode\":\"DAMAGED\"", "\"reasonCode\":\"LOST\""),
            HOLD.replace("\"quantity\":1", "\"quantity\":0"),
            HOLD.replace("\"holdId\":\"hold_51ab07\"", "\"holdId\":\"h\""),
            HOLD.replace("\"payload\":{", "\"payload\":[{").replace("}}", "}]}"),
        };
        for (String value : bad) {
            assertThrows(PoisonEventException.class, () -> PARSER.parse(value), value);
        }
    }
}
