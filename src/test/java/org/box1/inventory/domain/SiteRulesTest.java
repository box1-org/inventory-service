package org.box1.inventory.domain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.Test;

/** The SITE.md decisions as tables, without a database. Runs in CI. */
class SiteRulesTest {
    private static final Instant T = Instant.parse("2026-11-10T12:00:00Z");
    private static final String SITE = "site_ds9";

    private static Reservation hold(ReservationStatus status) {
        return new Reservation("res_1", "ord_1", SITE, "SKU-42", 2, status, T, null, null, UUID.randomUUID(), "c", null, T, T);
    }

    private static SiteEvent.Pick pick(String orderId, String sku, int quantity) {
        return new SiteEvent.Pick("pick_1", orderId, "res_1", sku, quantity, T);
    }

    @Test
    void pickOutcomesFollowTheReservationStatus() {
        SiteEvent.Pick matching = pick("ord_1", "SKU-42", 2);
        assertEquals(SiteRules.PickOutcome.PICKED, SiteRules.pick(Optional.of(hold(ReservationStatus.ACTIVE)), SITE, matching).outcome());
        assertEquals(SiteRules.PickOutcome.PICKED, SiteRules.pick(Optional.of(hold(ReservationStatus.COMMITTED)), SITE, matching).outcome());
        assertEquals(SiteRules.PickOutcome.PICKED_AFTER_SHIPMENT,
                SiteRules.pick(Optional.of(hold(ReservationStatus.CONSUMED)), SITE, matching).outcome());
        assertEquals(SiteRules.PickOutcome.NOT_HELD, SiteRules.pick(Optional.of(hold(ReservationStatus.EXPIRED)), SITE, matching).outcome());
        assertEquals(SiteRules.PickOutcome.NOT_HELD, SiteRules.pick(Optional.of(hold(ReservationStatus.RELEASED)), SITE, matching).outcome());
        SiteRules.PickDecision unknown = SiteRules.pick(Optional.empty(), SITE, matching);
        assertEquals(SiteRules.PickOutcome.MISMATCH, unknown.outcome());
        assertTrue(unknown.differs().isEmpty());
    }

    @Test
    void anyDifferingFieldIsAMismatchWhateverTheStatus() {
        for (ReservationStatus status : ReservationStatus.values()) {
            Optional<Reservation> held = Optional.of(hold(status));
            assertEquals(List.of("orderId"), SiteRules.pick(held, SITE, pick("ord_2", "SKU-42", 2)).differs());
            assertEquals(List.of("siteId"), SiteRules.pick(held, "site_spacedock", pick("ord_1", "SKU-42", 2)).differs());
            assertEquals(List.of("sku"), SiteRules.pick(held, SITE, pick("ord_1", "SKU-7", 2)).differs());
            assertEquals(List.of("quantity"), SiteRules.pick(held, SITE, pick("ord_1", "SKU-42", 3)).differs());
            assertEquals(SiteRules.PickOutcome.MISMATCH, SiteRules.pick(held, SITE, pick("ord_1", "SKU-42", 1)).outcome());
        }
    }

    @Test
    void onlyMismatchAndNotHeldOpenAnItem() {
        assertNull(SiteRules.PickOutcome.PICKED.reconciliationKind());
        assertNull(SiteRules.PickOutcome.PICKED_AFTER_SHIPMENT.reconciliationKind());
        assertEquals("PICK_NOT_HELD", SiteRules.PickOutcome.NOT_HELD.reconciliationKind());
        assertEquals("PICK_MISMATCH", SiteRules.PickOutcome.MISMATCH.reconciliationKind());
    }

    /** Columns: counted C, later movements D, reserved, on hand before; then E, on hand after, delta, conflict, shortfall. */
    @Test
    void theCountRuleTable() {
        long[][] table = {
            {17, 0, 0, 20, 17, 17, -3, 0, 0},     // plain count, units missing
            {17, 0, 0, 10, 17, 17, 7, 0, 0},      // plain count, units found
            {17, 0, 5, 17, 17, 17, 0, 0, 0},      // count agrees: a zero-delta movement
            {10, -2, 3, 9, 8, 8, -1, 0, 0},       // a shipment after the count already took 2
            {10, 4, 0, 10, 14, 14, 4, 0, 0},      // a restock after the count added 4
            {2, 0, 4, 5, 2, 4, -1, 1, 2},         // E below reserved: held at reserved, conflict
            {1, -3, 0, 2, -2, 0, -2, 1, 2},       // E negative: held at 0, conflict
            {0, -1, 3, 3, -1, 3, 0, 1, 4},        // E negative and below reserved: held at reserved
            {0, 0, 0, 0, 0, 0, 0, 0, 0},          // an empty bin counted empty
            {3, 0, 3, 7, 3, 3, -4, 0, 0},         // E equal to reserved is not a conflict
        };
        for (long[] row : table) {
            SiteRules.CountDecision d = SiteRules.count((int) row[0], row[1], (int) row[2], (int) row[3]);
            String name = java.util.Arrays.toString(row);
            assertEquals(row[4], d.expected(), name);
            assertEquals(row[5], d.onHandAfter(), name);
            assertEquals(row[6], d.delta(), name);
            assertEquals(row[7] == 1, d.conflict(), name);
            assertEquals(row[8], d.shortfall(), name);
            assertTrue(d.onHandAfter() >= d.reserved() && d.onHandAfter() >= 0, name);
        }
    }

    @Test
    void laterIsByCountedAtThenByCountId() {
        assertTrue(SiteRules.later(T.plusSeconds(1), "cnt_a", T, "cnt_z"));
        assertFalse(SiteRules.later(T, "cnt_z", T.plusSeconds(1), "cnt_a"));
        assertTrue(SiteRules.later(T, "cnt_b", T, "cnt_a"));
        assertFalse(SiteRules.later(T, "cnt_a", T, "cnt_b"));
        assertFalse(SiteRules.later(T, "cnt_a", T, "cnt_a"));
        // Byte order, as the database compares with COLLATE "C": upper case sorts before lower case.
        assertTrue(SiteRules.later(T, "cnt_a", T, "cnt_B"));
    }

    /** A movement in the model ledger: its delta and business time. */
    private record Movement(int delta, Instant at) {}

    /** The service's count path, in memory: supersede check, D, rule. Returns on hand after. */
    private static final class Model {
        final List<Movement> movements = new ArrayList<>();
        final List<Object[]> applied = new ArrayList<>();
        int onHand;
        final int reserved;

        Model(int onHand, int reserved) {
            this.onHand = onHand;
            this.reserved = reserved;
        }

        void count(String countId, int counted, Instant countedAt) {
            for (Object[] other : applied) {
                if (SiteRules.later((Instant) other[1], (String) other[0], countedAt, countId)) {
                    return;
                }
            }
            long later = movements.stream().filter(m -> m.at().isAfter(countedAt)).mapToLong(Movement::delta).sum();
            onHand = SiteRules.count(counted, later, reserved, onHand).onHandAfter();
            applied.add(new Object[] {countId, countedAt});
        }
    }

    @Test
    void twoCountsGiveTheSameOnHandInEitherArrivalOrder() {
        // Movements, reserved and the starting on hand are the same in both runs; only the arrival order differs.
        int[][] scenarios = {{20, 0, 15, 12}, {20, 5, 15, 3}, {8, 6, 2, 9}, {50, 10, 40, 40}, {5, 5, 0, 0}};
        for (int[] s : scenarios) {
            for (int gap : new int[] {0, 30, 3600}) {
                Instant ta = T;
                Instant tb = T.plusSeconds(gap);
                Model ab = new Model(s[0], s[1]);
                Model ba = new Model(s[0], s[1]);
                for (Model m : List.of(ab, ba)) {
                    m.movements.add(new Movement(-2, T.minusSeconds(10)));
                    m.movements.add(new Movement(-1, T.plusSeconds(gap / 2 + 1)));
                    m.movements.add(new Movement(4, T.plusSeconds(gap + 5)));
                }
                ab.count("cnt_a", s[2], ta);
                ab.count("cnt_b", s[3], tb);
                ba.count("cnt_b", s[3], tb);
                ba.count("cnt_a", s[2], ta);
                assertEquals(ab.onHand, ba.onHand, java.util.Arrays.toString(s) + " gap " + gap);
            }
        }
    }

    @Test
    void replayingACountChangesNothing() {
        Model m = new Model(20, 2);
        m.count("cnt_a", 15, T);
        int after = m.onHand;
        m.count("cnt_a", 15, T);
        assertEquals(after, m.onHand);
    }
}
