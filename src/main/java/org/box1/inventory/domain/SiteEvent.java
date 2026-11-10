package org.box1.inventory.domain;

import java.time.Instant;
import java.util.UUID;

/**
 * A validated event from box1.site.events.v1 (SITE.md in box1-ops-contracts). aggregateId is the siteId. The payload is
 * exactly one of a pick, a cycle count or a hold transition.
 */
public record SiteEvent(UUID eventId, String eventType, String siteId, Instant occurredAt, String correlationId,
        String traceparent, Payload payload) {
    public static final String PICK_CONFIRMED = "site.pick-confirmed.v1";
    public static final String STOCK_ADJUSTED = "site.stock-adjusted.v1";
    public static final String STOCK_HELD = "site.stock-held.v1";

    /** One site action. */
    public sealed interface Payload permits Pick, Count, Hold {}

    /** An operator confirmed that one reservation's units were picked. */
    public record Pick(String pickId, String orderId, String reservationId, String sku, int quantity, Instant pickedAt)
            implements Payload {}

    /** A cycle count: the units of one SKU physically at the site at countedAt. */
    public record Count(String countId, String sku, int countedQuantity, Instant countedAt, String reason) implements Payload {}

    /** A hold placed or released on units of one SKU. */
    public record Hold(String holdId, String sku, int quantity, String action, String reasonCode, Instant changedAt)
            implements Payload {}
}
