package org.box1.inventory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Optional;
import java.util.UUID;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.transaction.Transactional;
import org.box1.inventory.domain.InboundEvent;
import org.box1.inventory.domain.PoisonEventException;
import org.box1.inventory.domain.ReasonCode;
import org.box1.inventory.domain.RequestProblem;
import org.box1.inventory.domain.Reservation;
import org.box1.inventory.domain.ReservationStatus;
import org.box1.inventory.domain.StockLevel;
import org.box1.inventory.messaging.EventFactory;
import org.box1.inventory.store.InventoryStore;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Every business effect. Each public method is one transaction holding the stock change, the reservation, its history,
 * the dedupe record and the outbox row, so they commit or roll back together. Inventory never updates order records.
 */
@ApplicationScoped
public class ReservationService {
    private static final String CONSUMER = "consumer";
    private static final String SCHEDULER = "scheduler";

    private final InventoryStore store;
    private final EventFactory events;
    private final Clock clock;
    private final Duration ttl;

    public ReservationService(InventoryStore store, EventFactory events, Clock clock,
            @ConfigProperty(name = "inventory.reservation.ttl") String ttl) {
        this.store = store;
        this.events = events;
        this.clock = clock;
        this.ttl = Duration.parse(ttl);
    }

    /** Apply one inbound event once. A redelivered eventId has no effect. */
    @Transactional
    public Applied apply(InboundEvent event) {
        Instant now = clock.instant();
        if (!store.markProcessed(event.eventId(), event.eventType(), now)) {
            return Applied.of("duplicate");
        }
        return switch (event.eventType()) {
            case InboundEvent.ORDER_PLACED -> placed(event, now);
            case InboundEvent.FULFILLMENT_REQUESTED -> requested(event, now);
            case InboundEvent.STATE_CHANGED -> stateChanged(event, now);
            case InboundEvent.SHIPMENT_CONFIRMED -> shipped(event, now);
            case InboundEvent.SHIPMENT_FAILED -> failed(event, now);
            default -> throw new PoisonEventException("Unsupported eventType");
        };
    }

    private Applied placed(InboundEvent event, Instant now) {
        Optional<InventoryStore.Outcome> existing = store.outcome(event.orderId());
        if (existing.isPresent()) {
            InventoryStore.Outcome known = existing.get();
            if (!known.sku().equals(event.sku()) || known.quantity() != event.quantity()
                    || !known.siteId().equals(event.siteId())) {
                throw new PoisonEventException("Order differs from its recorded decision");
            }
            return Applied.of("duplicate_order");
        }
        String traceparent = EventFactory.traceparent(event.traceparent());
        if (store.tryHold(event.siteId(), event.sku(), event.quantity(), now)) {
            String reservationId = "res_" + UUID.randomUUID().toString().replace("-", "");
            Reservation hold = new Reservation(reservationId, event.orderId(), event.siteId(), event.sku(), event.quantity(),
                    ReservationStatus.ACTIVE, now.plus(ttl), null, null, event.eventId(), event.correlationId(), traceparent,
                    now, now);
            store.insertReservation(hold);
            store.recordOutcome(event.orderId(), new InventoryStore.Outcome("RESERVED", reservationId, null, event.siteId(),
                    event.sku(), event.quantity()), event.eventId(), now);
            store.history(reservationId, null, ReservationStatus.ACTIVE, CONSUMER, "ORDER_PLACED", event.eventId(), now);
            events.reserved(hold, event.eventId(), now);
            return new Applied("reserved", event.siteId(), event.quantity(), null);
        }
        ReasonCode reason = store.classifyDenial(event.siteId(), event.sku());
        store.recordOutcome(event.orderId(), new InventoryStore.Outcome("REJECTED", null, reason.name(), event.siteId(),
                event.sku(), event.quantity()), event.eventId(), now);
        events.rejected(event.orderId(), event.sku(), event.quantity(), event.siteId(), reason, event.correlationId(),
                event.eventId(), traceparent, now);
        return new Applied("rejected", event.siteId(), 0, reason);
    }

    private Applied requested(InboundEvent event, Instant now) {
        Optional<Reservation> hold = store.reservation(event.reservationId());
        if (hold.isEmpty() || !hold.get().orderId().equals(event.orderId())) {
            store.reconciliation(event.orderId(), event.reservationId(), "FULFILLMENT_WITHOUT_RESERVATION", false, event.eventId(), now);
            return Applied.of("reconciliation");
        }
        if (store.commit(event.reservationId(), event.fulfillmentRequestId(), now)) {
            store.history(event.reservationId(), ReservationStatus.ACTIVE, ReservationStatus.COMMITTED, CONSUMER,
                    "FULFILLMENT_REQUESTED", event.eventId(), now);
            return Applied.of("committed");
        }
        ReservationStatus status = store.reservation(event.reservationId()).orElseThrow().status();
        if (status == ReservationStatus.EXPIRED || status == ReservationStatus.RELEASED) {
            store.reconciliation(event.orderId(), event.reservationId(), "FULFILLMENT_AFTER_RELEASE", false, event.eventId(), now);
            return Applied.of("reconciliation");
        }
        return Applied.of("ignored");
    }

    private Applied stateChanged(InboundEvent event, Instant now) {
        if (!"CANCELLED".equals(event.toState())) {
            return Applied.of("ignored");
        }
        Optional<Reservation> hold = store.reservationForOrder(event.orderId());
        if (hold.isEmpty()) {
            return Applied.of("ignored");
        }
        Reservation value = hold.get();
        if (store.release(value.reservationId(), false, now)) {
            store.returnHold(value.siteId(), value.sku(), value.quantity(), now);
            store.history(value.reservationId(), ReservationStatus.ACTIVE, ReservationStatus.RELEASED, CONSUMER,
                    "ORDER_CANCELLED", event.eventId(), now);
            return Applied.of("released");
        }
        if (store.reservation(value.reservationId()).orElseThrow().status() == ReservationStatus.COMMITTED) {
            store.reconciliation(event.orderId(), value.reservationId(), "CANCELLED_AFTER_COMMIT", false, event.eventId(), now);
            return Applied.of("reconciliation");
        }
        return Applied.of("ignored");
    }

    private Applied shipped(InboundEvent event, Instant now) {
        Optional<Reservation> hold = store.reservationForOrder(event.orderId());
        if (hold.isEmpty()) {
            store.reconciliation(event.orderId(), null, "SHIPMENT_WITHOUT_RESERVATION", false, event.eventId(), now);
            return Applied.of("reconciliation");
        }
        Reservation value = hold.get();
        if (store.consume(value.reservationId(), event.shipmentId(), now)) {
            store.consumeHold(value.siteId(), value.sku(), value.quantity(), now);
            store.movement(value.siteId(), value.sku(), -value.quantity(), "SHIPMENT", CONSUMER, "SHIPMENT_CONFIRMED",
                    event.shipmentId(), now);
            store.history(value.reservationId(), value.status(), ReservationStatus.CONSUMED, CONSUMER, "SHIPMENT_CONFIRMED",
                    event.eventId(), now);
            return Applied.of("consumed");
        }
        Reservation current = store.reservation(value.reservationId()).orElseThrow();
        if (current.status() == ReservationStatus.CONSUMED) {
            if (event.shipmentId().equals(current.shipmentId())) {
                return Applied.of("duplicate_shipment");
            }
            store.reconciliation(event.orderId(), value.reservationId(), "SHIPMENT_DUPLICATE", false, event.eventId(), now);
            return Applied.of("reconciliation");
        }
        // The units physically left after the hold was given back, so count them if stock allows and leave a record.
        boolean adjusted = store.tryReduceOnHand(value.siteId(), value.sku(), value.quantity(), now);
        if (adjusted) {
            store.movement(value.siteId(), value.sku(), -value.quantity(), "LATE_SHIPMENT", CONSUMER, "SHIPMENT_AFTER_RELEASE",
                    event.shipmentId(), now);
        }
        store.reconciliation(event.orderId(), value.reservationId(), "SHIPMENT_AFTER_RELEASE", adjusted, event.eventId(), now);
        return Applied.of("reconciliation");
    }

    private Applied failed(InboundEvent event, Instant now) {
        if (event.retryable()) {
            return Applied.of("ignored");
        }
        // Goods may have moved, so the hold stays until a person decides.
        Optional<Reservation> hold = store.reservationForOrder(event.orderId());
        store.reconciliation(event.orderId(), hold.map(Reservation::reservationId).orElse(null), "SHIPMENT_FAILED", false,
                event.eventId(), now);
        return Applied.of("reconciliation");
    }

    /** Expire one due hold. Empty when another path changed it first. */
    @Transactional
    public Optional<Reservation> expire(String reservationId) {
        Instant now = clock.instant();
        if (!store.expire(reservationId, now)) {
            return Optional.empty();
        }
        Reservation hold = store.reservation(reservationId).orElseThrow();
        store.returnHold(hold.siteId(), hold.sku(), hold.quantity(), now);
        store.history(reservationId, ReservationStatus.ACTIVE, ReservationStatus.EXPIRED, SCHEDULER, "TTL_ELAPSED", null, now);
        events.expired(hold, now);
        return Optional.of(hold);
    }

    /** Operator release. A repeat returns the same result with no second effect. */
    @Transactional
    public Reservation release(String reservationId, String subject, String reason) {
        Instant now = clock.instant();
        Reservation hold = store.reservation(reservationId)
                .orElseThrow(() -> new RequestProblem(404, "Not Found", "Reservation not found"));
        if (store.release(reservationId, true, now)) {
            store.returnHold(hold.siteId(), hold.sku(), hold.quantity(), now);
            store.history(reservationId, hold.status(), ReservationStatus.RELEASED, subject, reason, null, now);
            return store.reservation(reservationId).orElseThrow();
        }
        Reservation current = store.reservation(reservationId).orElseThrow();
        if (current.status() == ReservationStatus.RELEASED) {
            return current;
        }
        throw new RequestProblem(409, "Conflict", "Reservation is " + current.status().name() + " and cannot be released");
    }

    /** Add stock under an idempotency key. A repeat of the same request changes nothing. */
    @Transactional
    public StockLevel restock(String siteId, String sku, int quantity, String reason, String key, String subject) {
        Instant now = clock.instant();
        if (store.stock(siteId, sku, 1, 0).isEmpty()) {
            throw new RequestProblem(404, "Not Found", "Stock record not found");
        }
        String hash = hash(siteId, sku, Integer.toString(quantity), reason);
        if (store.claimRestock(siteId, sku, quantity, subject, reason, key, hash, now)) {
            store.addOnHand(siteId, sku, quantity, now);
        } else if (!hash.equals(store.restockHash(key))) {
            throw new RequestProblem(409, "Conflict", "Idempotency-Key was used with a different request");
        }
        return store.stock(siteId, sku, 1, 0).get(0);
    }

    private static String hash(String... parts) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (String part : parts) {
                byte[] bytes = part.getBytes(StandardCharsets.UTF_8);
                digest.update(Integer.toString(bytes.length).getBytes(StandardCharsets.UTF_8));
                digest.update((byte) ':');
                digest.update(bytes);
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException(error);
        }
    }

    /** What an event did, for metrics after commit. */
    public record Applied(String outcome, String siteId, int units, ReasonCode reason) {
        static Applied of(String outcome) {
            return new Applied(outcome, null, 0, null);
        }
    }
}
