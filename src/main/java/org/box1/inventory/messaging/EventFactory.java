package org.box1.inventory.messaging;

import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import jakarta.enterprise.context.ApplicationScoped;
import org.box1.inventory.domain.ReasonCode;
import org.box1.inventory.domain.Reservation;
import org.box1.inventory.store.OutboxStore;

/** Builds contract envelopes and enqueues them in the caller's transaction. */
@ApplicationScoped
public class EventFactory {
    public static final String RESERVED = "inventory.reserved.v1";
    public static final String REJECTED = "inventory.rejected.v1";
    public static final String EXPIRED = "inventory.reservation-expired.v1";

    private final ObjectMapper mapper;
    private final OutboxStore outbox;

    public EventFactory(ObjectMapper mapper, OutboxStore outbox) {
        this.mapper = mapper;
        this.outbox = outbox;
    }

    /** Enqueue inventory.reserved.v1. */
    public void reserved(Reservation hold, UUID causationId, Instant now) {
        ObjectNode payload = mapper.createObjectNode();
        payload.put("orderId", hold.orderId());
        payload.put("reservationId", hold.reservationId());
        payload.put("sku", hold.sku());
        payload.put("quantity", hold.quantity());
        payload.put("siteId", hold.siteId());
        payload.put("expiresAt", hold.expiresAt().toString());
        enqueue(RESERVED, hold.orderId(), hold.correlationId(), causationId, hold.traceparent(), payload, now);
    }

    /** Enqueue inventory.rejected.v1. */
    public void rejected(String orderId, String sku, int quantity, String siteId, ReasonCode reason, String correlationId,
            UUID causationId, String traceparent, Instant now) {
        ObjectNode payload = mapper.createObjectNode();
        payload.put("orderId", orderId);
        payload.put("sku", sku);
        payload.put("quantity", quantity);
        payload.put("siteId", siteId);
        payload.put("reasonCode", reason.name());
        enqueue(REJECTED, orderId, correlationId, causationId, traceparent, payload, now);
    }

    /** Enqueue inventory.reservation-expired.v1. Its trace follows the order that placed the hold. */
    public void expired(Reservation hold, Instant now) {
        ObjectNode payload = mapper.createObjectNode();
        payload.put("orderId", hold.orderId());
        payload.put("reservationId", hold.reservationId());
        payload.put("expiredAt", now.toString());
        enqueue(EXPIRED, hold.orderId(), hold.correlationId(), hold.placedEventId(), hold.traceparent(), payload, now);
    }

    /** The active span as W3C traceparent, or the inbound one when no span is recording. */
    public static String traceparent(String inbound) {
        SpanContext context = Span.current().getSpanContext();
        if (!context.isValid()) {
            return inbound;
        }
        return "00-" + context.getTraceId() + "-" + context.getSpanId() + "-" + context.getTraceFlags().asHex();
    }

    private void enqueue(String type, String orderId, String correlationId, UUID causationId, String traceparent,
            ObjectNode payload, Instant now) {
        UUID eventId = UUID.randomUUID();
        outbox.enqueue(eventId, orderId, type, envelope(eventId, type, orderId, correlationId, causationId, traceparent,
                payload, now).toString());
    }

    /** Build an envelope matching events/v1/envelope.schema.json. */
    ObjectNode envelope(UUID eventId, String type, String orderId, String correlationId, UUID causationId,
            String traceparent, ObjectNode payload, Instant now) {
        ObjectNode envelope = mapper.createObjectNode();
        envelope.put("eventId", eventId.toString());
        envelope.put("eventType", type);
        envelope.put("schemaVersion", 1);
        envelope.put("aggregateId", orderId);
        envelope.put("occurredAt", now.toString());
        envelope.put("correlationId", correlationId);
        envelope.put("causationId", causationId.toString());
        if (traceparent != null) {
            envelope.put("traceparent", traceparent);
        }
        envelope.set("payload", payload);
        return envelope;
    }
}
