package org.box1.inventory.messaging;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.enterprise.context.ApplicationScoped;
import org.box1.inventory.domain.PoisonEventException;
import org.box1.inventory.domain.SiteEvent;

/**
 * Validates records from box1.site.events.v1 against the site schemas in box1-ops-contracts, with the same envelope
 * rules as {@link EventParser}. A record of another eventType is not poison: it is returned empty, to be acknowledged
 * and ignored. Failure messages name fields and never echo values, because they reach dead-letter headers.
 */
@ApplicationScoped
public class SiteEventParser {
    public static final Set<String> SITE_TYPES = Set.of(SiteEvent.PICK_CONFIRMED, SiteEvent.STOCK_ADJUSTED,
            SiteEvent.STOCK_HELD);

    private static final Pattern PICK = Pattern.compile("^pick_[A-Za-z0-9_-]{1,64}$");
    private static final Pattern COUNT = Pattern.compile("^cnt_[A-Za-z0-9_-]{1,64}$");
    private static final Pattern HOLD = Pattern.compile("^hold_[A-Za-z0-9_-]{1,64}$");
    private static final Set<String> COUNT_REASONS = Set.of("CYCLE_COUNT");
    private static final Set<String> HOLD_ACTIONS = Set.of("PLACED", "RELEASED");
    private static final Set<String> HOLD_REASONS = Set.of("DAMAGED", "QUALITY_CHECK", "COUNT_DISCREPANCY", "OTHER");
    private static final int MAX_UNITS = 1_000_000;

    private final EventParser envelope;

    public SiteEventParser(EventParser envelope) {
        this.envelope = envelope;
    }

    /** Parse a record value. Empty for an event type this consumer does not handle; poison when invalid. */
    public Optional<SiteEvent> parse(String value) {
        JsonNode root = envelope.read(value);
        String type = EventParser.text(root, "eventType");
        if (!SITE_TYPES.contains(type)) {
            return Optional.empty();
        }
        JsonNode version = root.path("schemaVersion");
        if (!version.isInt() || version.intValue() != 1) {
            throw new PoisonEventException("Unsupported schemaVersion");
        }
        UUID eventId = EventParser.uuid(root);
        String aggregateId = EventParser.bounded(root, "aggregateId");
        Instant occurredAt = EventParser.instant(root, "occurredAt");
        String correlationId = EventParser.bounded(root, "correlationId");
        EventParser.bounded(root, "causationId");
        String traceparent = root.hasNonNull("traceparent") ? EventParser.matching(root, "traceparent", EventParser.TRACEPARENT) : null;
        JsonNode payload = root.path("payload");
        if (!payload.isObject()) {
            throw new PoisonEventException("Missing payload");
        }
        String siteId = EventParser.matching(payload, "siteId", EventParser.SITE);
        if (!siteId.equals(aggregateId)) {
            throw new PoisonEventException("aggregateId does not match siteId");
        }
        SiteEvent.Payload body = switch (type) {
            case SiteEvent.PICK_CONFIRMED -> new SiteEvent.Pick(EventParser.matching(payload, "pickId", PICK),
                    EventParser.matching(payload, "orderId", EventParser.ORDER),
                    EventParser.matching(payload, "reservationId", EventParser.RESERVATION),
                    EventParser.matching(payload, "sku", EventParser.SKU), units(payload, "quantity", 1, Integer.MAX_VALUE),
                    EventParser.instant(payload, "pickedAt"));
            case SiteEvent.STOCK_ADJUSTED -> new SiteEvent.Count(EventParser.matching(payload, "countId", COUNT),
                    EventParser.matching(payload, "sku", EventParser.SKU), units(payload, "countedQuantity", 0, MAX_UNITS),
                    EventParser.instant(payload, "countedAt"), oneOf(payload, "reason", COUNT_REASONS));
            case SiteEvent.STOCK_HELD -> new SiteEvent.Hold(EventParser.matching(payload, "holdId", HOLD),
                    EventParser.matching(payload, "sku", EventParser.SKU), units(payload, "quantity", 1, MAX_UNITS),
                    oneOf(payload, "action", HOLD_ACTIONS), oneOf(payload, "reasonCode", HOLD_REASONS),
                    EventParser.instant(payload, "changedAt"));
            default -> throw new PoisonEventException("Unsupported eventType");
        };
        return Optional.of(new SiteEvent(eventId, type, siteId, occurredAt, correlationId, traceparent, body));
    }

    private static int units(JsonNode payload, String field, int min, int max) {
        JsonNode value = payload.get(field);
        if (value == null || !value.isIntegralNumber() || !value.canConvertToInt() || value.asInt() < min || value.asInt() > max) {
            throw new PoisonEventException("Invalid " + field);
        }
        return value.asInt();
    }

    private static String oneOf(JsonNode payload, String field, Set<String> allowed) {
        String value = EventParser.text(payload, field);
        if (!allowed.contains(value)) {
            throw new PoisonEventException("Invalid " + field);
        }
        return value;
    }
}
