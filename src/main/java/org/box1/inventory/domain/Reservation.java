package org.box1.inventory.domain;

import java.time.Instant;
import java.util.UUID;

/** A stock hold for one order. */
public record Reservation(String reservationId, String orderId, String siteId, String sku, int quantity,
        ReservationStatus status, Instant expiresAt, String fulfillmentRequestId, String shipmentId,
        UUID placedEventId, String correlationId, String traceparent, Instant createdAt, Instant updatedAt) {}
