package org.box1.inventory;

import java.time.Clock;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.transaction.Transactional;
import org.box1.inventory.domain.Reservation;
import org.box1.inventory.domain.SiteEvent;
import org.box1.inventory.domain.SiteRules;
import org.box1.inventory.store.InventoryStore;
import org.box1.inventory.store.SiteStore;

/**
 * Applies site events as SITE.md ("Effect in inventory-service") states. Each event is one transaction holding the
 * processed_event record, the site record, any stock change and any reconciliation item. A site event never creates,
 * releases or consumes a reservation, never changes reserved, and publishes nothing.
 */
@ApplicationScoped
public class SiteEventService {
    static final String COUNT_CONFLICT = "COUNT_CONFLICT";
    static final String COUNT_UNKNOWN_STOCK = "COUNT_UNKNOWN_STOCK";
    static final String HOLD_UNKNOWN = "HOLD_UNKNOWN";
    private static final String ACTOR = "site-consumer";

    private final InventoryStore inventory;
    private final SiteStore store;
    private final ObjectMapper mapper;
    private final Clock clock;

    public SiteEventService(InventoryStore inventory, SiteStore store, ObjectMapper mapper, Clock clock) {
        this.inventory = inventory;
        this.store = store;
        this.mapper = mapper;
        this.clock = clock;
    }

    /** Apply one site event once. A redelivered eventId, or a repeated pickId, countId or hold transition, has no effect. */
    @Transactional
    public String apply(SiteEvent event) {
        Instant now = clock.instant();
        if (!inventory.markProcessed(event.eventId(), event.eventType(), now)) {
            return "duplicate";
        }
        return switch (event.payload()) {
            case SiteEvent.Pick pick -> pick(event, pick, now);
            case SiteEvent.Count count -> count(event, count, now);
            case SiteEvent.Hold hold -> hold(event, hold, now);
        };
    }

    private String pick(SiteEvent event, SiteEvent.Pick pick, Instant now) {
        if (store.pickExists(pick.pickId())) {
            return "duplicate_pick";
        }
        Optional<Reservation> reservation = inventory.reservation(pick.reservationId());
        SiteRules.PickDecision decision = SiteRules.pick(reservation, event.siteId(), pick);
        String status = reservation.map(r -> r.status().name()).orElse(null);
        store.recordPick(event.eventId(), event.siteId(), pick, decision.outcome(), status, now);
        String kind = decision.outcome().reconciliationKind();
        if (kind != null) {
            Map<String, Object> detail = new LinkedHashMap<>();
            detail.put("pickId", pick.pickId());
            detail.put("reservationStatus", status);
            if (reservation.isEmpty()) {
                detail.put("reservation", "UNKNOWN");
            }
            if (!decision.differs().isEmpty()) {
                detail.put("differs", decision.differs());
            }
            detail.put("quantity", pick.quantity());
            store.reconciliation(pick.orderId(), pick.reservationId(), kind, event.siteId(), pick.sku(), json(detail),
                    event.eventId(), now);
        }
        return "pick_" + decision.outcome().name().toLowerCase(java.util.Locale.ROOT);
    }

    private String count(SiteEvent event, SiteEvent.Count count, Instant now) {
        if (store.countExists(count.countId())) {
            return "duplicate_count";
        }
        Optional<SiteStore.StockRow> stock = store.lockStock(event.siteId(), count.sku());
        if (stock.isEmpty()) {
            store.recordCount(event.eventId(), event.siteId(), count, "UNKNOWN_STOCK", null, now);
            Map<String, Object> detail = new LinkedHashMap<>();
            detail.put("countId", count.countId());
            detail.put("countedQuantity", count.countedQuantity());
            detail.put("countedAt", count.countedAt().toString());
            store.reconciliation(null, null, COUNT_UNKNOWN_STOCK, event.siteId(), count.sku(), json(detail), event.eventId(), now);
            return "count_unknown_stock";
        }
        if (store.laterCountApplied(event.siteId(), count.sku(), count.countedAt(), count.countId())) {
            store.recordCount(event.eventId(), event.siteId(), count, "SUPERSEDED", null, now);
            return "count_superseded";
        }
        long later = store.movementsAfter(event.siteId(), count.sku(), count.countedAt());
        SiteRules.CountDecision decision = SiteRules.count(count.countedQuantity(), later, stock.get().reserved(),
                stock.get().onHand());
        if (decision.delta() != 0) {
            store.setOnHand(event.siteId(), count.sku(), decision.onHandAfter(), now);
        }
        store.countMovement(event.siteId(), count, decision.delta(), ACTOR);
        store.recordCount(event.eventId(), event.siteId(), count, "APPLIED", decision, now);
        if (decision.conflict()) {
            Map<String, Object> detail = new LinkedHashMap<>();
            detail.put("countId", count.countId());
            detail.put("countedQuantity", count.countedQuantity());
            detail.put("countedAt", count.countedAt().toString());
            detail.put("laterMovements", decision.laterMovements());
            detail.put("expected", decision.expected());
            detail.put("reserved", decision.reserved());
            detail.put("shortfall", decision.shortfall());
            detail.put("onHandAfter", decision.onHandAfter());
            store.reconciliation(null, null, COUNT_CONFLICT, event.siteId(), count.sku(), json(detail), event.eventId(), now);
            return "count_conflict";
        }
        return "count_applied";
    }

    private String hold(SiteEvent event, SiteEvent.Hold hold, Instant now) {
        if (store.holdExists(hold.holdId(), hold.action())) {
            return "duplicate_hold";
        }
        if ("RELEASED".equals(hold.action()) && !store.holdExists(hold.holdId(), "PLACED")) {
            store.recordHold(event.eventId(), event.siteId(), hold, "UNKNOWN_HOLD", now);
            Map<String, Object> detail = new LinkedHashMap<>();
            detail.put("holdId", hold.holdId());
            detail.put("quantity", hold.quantity());
            detail.put("reasonCode", hold.reasonCode());
            store.reconciliation(null, null, HOLD_UNKNOWN, event.siteId(), hold.sku(), json(detail), event.eventId(), now);
            return "hold_unknown";
        }
        store.recordHold(event.eventId(), event.siteId(), hold, "RECORDED", now);
        return "hold_" + hold.action().toLowerCase(java.util.Locale.ROOT);
    }

    private String json(Map<String, Object> detail) {
        try {
            return mapper.writeValueAsString(detail);
        } catch (JsonProcessingException error) {
            throw new IllegalStateException(error);
        }
    }
}
