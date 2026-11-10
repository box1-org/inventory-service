package org.box1.inventory.domain;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The decisions SITE.md ("Effect in inventory-service") prescribes for site events, as pure functions so they can be
 * tested without a database. SiteEventService applies them inside one transaction.
 */
public final class SiteRules {
    private SiteRules() {}

    /** What a pick confirmation is recorded as. Only MISMATCH and NOT_HELD open a reconciliation item. */
    public enum PickOutcome {
        PICKED(null),
        PICKED_AFTER_SHIPMENT(null),
        NOT_HELD("PICK_NOT_HELD"),
        MISMATCH("PICK_MISMATCH");

        private final String reconciliationKind;

        PickOutcome(String reconciliationKind) {
            this.reconciliationKind = reconciliationKind;
        }

        /** The reconciliation kind this outcome opens, or null. */
        public String reconciliationKind() {
            return reconciliationKind;
        }
    }

    /** A pick decision and, for a mismatch, the fields that differ (empty when the reservation is unknown). */
    public record PickDecision(PickOutcome outcome, List<String> differs) {
        public PickDecision {
            differs = List.copyOf(differs);
        }
    }

    /**
     * Classify a pick against the reservation it names. Identity is checked first: a pick whose orderId, siteId, sku or
     * quantity differs from the reservation is a mismatch whatever the reservation's status, because it does not
     * describe that reservation. A matching pick is then judged by status.
     */
    public static PickDecision pick(Optional<Reservation> reservation, String siteId, SiteEvent.Pick pick) {
        if (reservation.isEmpty()) {
            return new PickDecision(PickOutcome.MISMATCH, List.of());
        }
        Reservation held = reservation.get();
        List<String> differs = new ArrayList<>();
        if (!held.orderId().equals(pick.orderId())) {
            differs.add("orderId");
        }
        if (!held.siteId().equals(siteId)) {
            differs.add("siteId");
        }
        if (!held.sku().equals(pick.sku())) {
            differs.add("sku");
        }
        if (held.quantity() != pick.quantity()) {
            differs.add("quantity");
        }
        if (!differs.isEmpty()) {
            return new PickDecision(PickOutcome.MISMATCH, differs);
        }
        return switch (held.status()) {
            case ACTIVE, COMMITTED -> new PickDecision(PickOutcome.PICKED, List.of());
            case CONSUMED -> new PickDecision(PickOutcome.PICKED_AFTER_SHIPMENT, List.of());
            case EXPIRED, RELEASED -> new PickDecision(PickOutcome.NOT_HELD, List.of());
        };
    }

    /**
     * True when count A (atA, idA) is later than count B: a later countedAt, or on a tie the greater countId. Two counts
     * with the same countId are the same count and neither is later.
     */
    public static boolean later(Instant atA, String idA, Instant atB, String idB) {
        int byTime = atA.compareTo(atB);
        return byTime > 0 || byTime == 0 && idA.compareTo(idB) > 0;
    }

    /**
     * The count rule. expected E = counted + laterMovements, where laterMovements D is the sum of the site and SKU's
     * non-count stock movements whose business time is after countedAt. on_hand becomes max(E, reserved, 0). When E is
     * below reserved, or negative, on_hand is held at that limit and the decision is a conflict with this shortfall.
     */
    public static CountDecision count(int counted, long laterMovements, int reserved, int onHand) {
        long expected = counted + laterMovements;
        long target = Math.max(expected, Math.max(reserved, 0L));
        boolean conflict = expected < reserved || expected < 0;
        return new CountDecision(expected, laterMovements, reserved, onHand, Math.toIntExact(target), conflict,
                conflict ? target - expected : 0);
    }

    /** The outcome of the count rule. delta is what the COUNT stock movement records, and may be zero. */
    public record CountDecision(long expected, long laterMovements, int reserved, int onHandBefore, int onHandAfter,
            boolean conflict, long shortfall) {
        public int delta() {
            return onHandAfter - onHandBefore;
        }
    }
}
