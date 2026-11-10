package org.box1.inventory.domain;

import java.time.Instant;
import java.util.UUID;

/**
 * A validated inbound event. customerId is checked for presence but deliberately never kept. shippedAt is set only for
 * shipment.confirmed.v1: it is the business time of the shipment's stock movement, which the site count rule reads.
 */
public record InboundEvent(UUID eventId, String eventType, String orderId, Instant occurredAt, String correlationId,
        String traceparent, String sku, Integer quantity, String siteId, String reservationId,
        String fulfillmentRequestId, String shipmentId, String toState, boolean retryable, Instant shippedAt) {
    public static final String ORDER_PLACED = "order.placed.v1";
    public static final String FULFILLMENT_REQUESTED = "fulfillment.requested.v1";
    public static final String STATE_CHANGED = "order.state-changed.v1";
    public static final String SHIPMENT_CONFIRMED = "shipment.confirmed.v1";
    public static final String SHIPMENT_FAILED = "shipment.failed.v1";
}
