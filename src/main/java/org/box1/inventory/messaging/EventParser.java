package org.box1.inventory.messaging;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import org.box1.inventory.domain.InboundEvent;
import org.box1.inventory.domain.PoisonEventException;

/**
 * Validates inbound envelopes and payloads against the rules in box1-ops-contracts. Failure messages name fields and
 * never echo values, because they reach dead-letter headers. Unknown payload fields are tolerated, as additive v1 changes.
 */
@ApplicationScoped
public class EventParser {
    public static final Set<String> ORDER_TOPIC_TYPES = Set.of(InboundEvent.ORDER_PLACED,
            InboundEvent.FULFILLMENT_REQUESTED, InboundEvent.STATE_CHANGED);
    public static final Set<String> FULFILLMENT_TOPIC_TYPES = Set.of(InboundEvent.SHIPMENT_CONFIRMED,
            InboundEvent.SHIPMENT_FAILED);

    static final Pattern ORDER = Pattern.compile("^ord_[A-Za-z0-9_-]{1,124}$");
    static final Pattern RESERVATION = Pattern.compile("^res_[A-Za-z0-9_-]{1,124}$");
    private static final Pattern FULFILLMENT = Pattern.compile("^ful_[A-Za-z0-9_-]{1,124}$");
    private static final Pattern SHIPMENT = Pattern.compile("^shp_[A-Za-z0-9_-]{1,124}$");
    static final Pattern SITE = Pattern.compile("^site_[A-Za-z0-9_-]{1,120}$");
    static final Pattern SKU = Pattern.compile("^[^\\p{Cntrl}]{1,80}$");
    private static final Pattern CUSTOMER = Pattern.compile("^[^\\p{Cntrl}]{1,128}$");
    private static final Pattern STATE = Pattern.compile("^[A-Z][A-Z_]{0,63}$");
    private static final Pattern REASON = Pattern.compile("^[A-Z][A-Z0-9_]{0,63}$");
    static final Pattern TRACEPARENT = Pattern.compile("^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$");
    private static final Pattern BOUNDED_ID = Pattern.compile("^[^\\p{Cntrl}]{1,128}$");
    private static final Pattern UUID_FORM = Pattern.compile("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");

    private final ObjectMapper mapper;

    public EventParser(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    /** Parse a record value, or throw {@link PoisonEventException}. */
    public InboundEvent parse(String value, Set<String> allowedTypes) {
        JsonNode root = read(value);
        JsonNode version = root.path("schemaVersion");
        if (!version.isInt() || version.intValue() != 1) {
            throw new PoisonEventException("Unsupported schemaVersion");
        }
        UUID eventId = uuid(root);
        String type = text(root, "eventType");
        if (!allowedTypes.contains(type)) {
            throw new PoisonEventException("Unsupported eventType");
        }
        String aggregateId = bounded(root, "aggregateId");
        Instant occurredAt = instant(root, "occurredAt");
        String correlationId = bounded(root, "correlationId");
        bounded(root, "causationId");
        String traceparent = root.hasNonNull("traceparent") ? matching(root, "traceparent", TRACEPARENT) : null;
        JsonNode payload = root.path("payload");
        if (!payload.isObject()) {
            throw new PoisonEventException("Missing payload");
        }
        String orderId = matching(payload, "orderId", ORDER);
        if (!orderId.equals(aggregateId)) {
            throw new PoisonEventException("aggregateId does not match orderId");
        }
        String sku = null;
        Integer quantity = null;
        String siteId = null;
        String reservationId = null;
        String fulfillmentRequestId = null;
        String shipmentId = null;
        String toState = null;
        boolean retryable = false;
        Instant shippedAt = null;
        switch (type) {
            case InboundEvent.ORDER_PLACED -> {
                matching(payload, "customerId", CUSTOMER);
                sku = matching(payload, "sku", SKU);
                quantity = quantity(payload);
                siteId = matching(payload, "siteId", SITE);
            }
            case InboundEvent.FULFILLMENT_REQUESTED -> {
                reservationId = matching(payload, "reservationId", RESERVATION);
                fulfillmentRequestId = matching(payload, "fulfillmentRequestId", FULFILLMENT);
                sku = matching(payload, "sku", SKU);
                quantity = quantity(payload);
                siteId = matching(payload, "siteId", SITE);
            }
            case InboundEvent.STATE_CHANGED -> {
                matching(payload, "fromState", STATE);
                toState = matching(payload, "toState", STATE);
                matching(payload, "reasonCode", REASON);
                instant(payload, "changedAt");
            }
            case InboundEvent.SHIPMENT_CONFIRMED -> {
                fulfillmentRequestId = matching(payload, "fulfillmentRequestId", FULFILLMENT);
                shipmentId = matching(payload, "shipmentId", SHIPMENT);
                shippedAt = instant(payload, "shippedAt");
            }
            case InboundEvent.SHIPMENT_FAILED -> {
                fulfillmentRequestId = matching(payload, "fulfillmentRequestId", FULFILLMENT);
                matching(payload, "reasonCode", REASON);
                if (!payload.path("retryable").isBoolean()) {
                    throw new PoisonEventException("Missing retryable");
                }
                retryable = payload.get("retryable").asBoolean();
            }
            default -> throw new PoisonEventException("Unsupported eventType");
        }
        return new InboundEvent(eventId, type, orderId, occurredAt, correlationId, traceparent, sku, quantity, siteId,
                reservationId, fulfillmentRequestId, shipmentId, toState, retryable, shippedAt);
    }

    private static Integer quantity(JsonNode payload) {
        JsonNode value = payload.get("quantity");
        if (value == null || !value.isIntegralNumber() || !value.canConvertToInt() || value.asInt() < 1) {
            throw new PoisonEventException("Invalid quantity");
        }
        return value.asInt();
    }

    JsonNode read(String value) {
        if (value == null) {
            throw new PoisonEventException("Empty record value");
        }
        try {
            JsonNode root = mapper.readTree(value);
            if (root == null || !root.isObject()) {
                throw new PoisonEventException("Record value is not a JSON object");
            }
            return root;
        } catch (JsonProcessingException error) {
            throw new PoisonEventException("Record value is not valid JSON");
        }
    }

    static UUID uuid(JsonNode root) {
        String value = text(root, "eventId");
        if (!UUID_FORM.matcher(value).matches()) {
            throw new PoisonEventException("Invalid eventId");
        }
        return UUID.fromString(value);
    }

    static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || !value.isTextual() || value.asText().isEmpty()) {
            throw new PoisonEventException("Missing " + field);
        }
        return value.asText();
    }

    static String bounded(JsonNode node, String field) {
        return matching(node, field, BOUNDED_ID);
    }

    static String matching(JsonNode node, String field, Pattern pattern) {
        String value = text(node, field);
        if (!pattern.matcher(value).matches()) {
            throw new PoisonEventException("Invalid " + field);
        }
        return value;
    }

    static Instant instant(JsonNode node, String field) {
        try {
            return Instant.parse(text(node, field));
        } catch (DateTimeParseException error) {
            throw new PoisonEventException("Invalid " + field);
        }
    }
}
