package org.box1.inventory.store;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import javax.sql.DataSource;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.transaction.Transactional;
import org.box1.inventory.domain.SiteEvent;
import org.box1.inventory.domain.SiteRules;

/** SQL for site events (V4). Writes join the caller's transaction, which also holds the processed_event record. */
@ApplicationScoped
public class SiteStore {
    private final DataSource dataSource;

    public SiteStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /** Whether a pick with this pickId is already recorded. */
    @Transactional(Transactional.TxType.MANDATORY)
    public boolean pickExists(String pickId) {
        return exists("SELECT 1 FROM site_pick WHERE pick_id = ?", pickId);
    }

    /** Whether a count with this countId is already recorded, whatever its outcome. */
    @Transactional(Transactional.TxType.MANDATORY)
    public boolean countExists(String countId) {
        return exists("SELECT 1 FROM site_count WHERE count_id = ?", countId);
    }

    /** Whether this hold transition is already recorded. */
    @Transactional(Transactional.TxType.MANDATORY)
    public boolean holdExists(String holdId, String action) {
        return exists("SELECT 1 FROM site_hold WHERE hold_id = ? AND action = ?", holdId, action);
    }

    private boolean exists(String sql, String... keys) {
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            for (int i = 0; i < keys.length; i++) {
                s.setString(i + 1, keys[i]);
            }
            try (ResultSet r = s.executeQuery()) {
                return r.next();
            }
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /** Record a pick decision. */
    @Transactional(Transactional.TxType.MANDATORY)
    public void recordPick(UUID eventId, String siteId, SiteEvent.Pick pick, SiteRules.PickOutcome outcome,
            String reservationStatus, Instant now) {
        String sql = "INSERT INTO site_pick (pick_id, event_id, site_id, order_id, reservation_id, sku, quantity, picked_at,"
                + " outcome, reservation_status, recorded_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setString(1, pick.pickId());
            s.setObject(2, eventId);
            s.setString(3, siteId);
            s.setString(4, pick.orderId());
            s.setString(5, pick.reservationId());
            s.setString(6, pick.sku());
            s.setInt(7, pick.quantity());
            s.setTimestamp(8, Timestamp.from(pick.pickedAt()));
            s.setString(9, outcome.name());
            s.setString(10, reservationStatus);
            s.setTimestamp(11, Timestamp.from(now));
            s.executeUpdate();
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /** Lock a stock row for the count rule, so no stock change interleaves with it. Empty when there is no row. */
    @Transactional(Transactional.TxType.MANDATORY)
    public Optional<StockRow> lockStock(String siteId, String sku) {
        String sql = "SELECT on_hand, reserved FROM inventory_stock WHERE site_id = ? AND sku = ? FOR UPDATE";
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setString(1, siteId);
            s.setString(2, sku);
            try (ResultSet r = s.executeQuery()) {
                return r.next() ? Optional.of(new StockRow(r.getInt(1), r.getInt(2))) : Optional.empty();
            }
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /** Whether a count of this site and SKU later than (countedAt, countId) has been applied. */
    @Transactional(Transactional.TxType.MANDATORY)
    public boolean laterCountApplied(String siteId, String sku, Instant countedAt, String countId) {
        String sql = "SELECT 1 FROM site_count WHERE site_id = ? AND sku = ? AND outcome = 'APPLIED'"
                + " AND (counted_at > ? OR (counted_at = ? AND count_id COLLATE \"C\" > ?)) LIMIT 1";
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setString(1, siteId);
            s.setString(2, sku);
            s.setTimestamp(3, Timestamp.from(countedAt));
            s.setTimestamp(4, Timestamp.from(countedAt));
            s.setString(5, countId);
            try (ResultSet r = s.executeQuery()) {
                return r.next();
            }
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /**
     * D in the count rule: the sum of a site and SKU's stock movements, other than counts, whose business time is after
     * the given time. Business time is business_at where set (shipments: shippedAt), otherwise occurred_at.
     */
    @Transactional(Transactional.TxType.MANDATORY)
    public long movementsAfter(String siteId, String sku, Instant after) {
        String sql = "SELECT COALESCE(sum(delta), 0) FROM stock_movement WHERE site_id = ? AND sku = ? AND kind <> 'COUNT'"
                + " AND COALESCE(business_at, occurred_at) > ?";
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setString(1, siteId);
            s.setString(2, sku);
            s.setTimestamp(3, Timestamp.from(after));
            try (ResultSet r = s.executeQuery()) {
                r.next();
                return r.getLong(1);
            }
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /** Set on hand to the count rule's result. The caller holds the row lock and keeps on_hand at or above reserved. */
    @Transactional(Transactional.TxType.MANDATORY)
    public void setOnHand(String siteId, String sku, int onHand, Instant now) {
        String sql = "UPDATE inventory_stock SET on_hand = ?, updated_at = ? WHERE site_id = ? AND sku = ?";
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setInt(1, onHand);
            s.setTimestamp(2, Timestamp.from(now));
            s.setString(3, siteId);
            s.setString(4, sku);
            s.executeUpdate();
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /**
     * Append the COUNT movement. Its time and business time are countedAt; the idempotency key is derived from countId,
     * so a second movement for one count cannot exist.
     */
    @Transactional(Transactional.TxType.MANDATORY)
    public void countMovement(String siteId, SiteEvent.Count count, int delta, String actor) {
        String sql = "INSERT INTO stock_movement (site_id, sku, delta, kind, actor, reason, reference, idempotency_key,"
                + " occurred_at, business_at) VALUES (?, ?, ?, 'COUNT', ?, ?, ?, ?, ?, ?)";
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setString(1, siteId);
            s.setString(2, count.sku());
            s.setInt(3, delta);
            s.setString(4, actor);
            s.setString(5, "SITE_" + count.reason());
            s.setString(6, count.countId());
            s.setString(7, countKey(count.countId()));
            s.setTimestamp(8, Timestamp.from(count.countedAt()));
            s.setTimestamp(9, Timestamp.from(count.countedAt()));
            s.executeUpdate();
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /** The stock_movement idempotency key of a count. */
    public static String countKey(String countId) {
        return "site-count:" + countId;
    }

    /** Record a count decision. decision is null unless the count was applied. */
    @Transactional(Transactional.TxType.MANDATORY)
    public void recordCount(UUID eventId, String siteId, SiteEvent.Count count, String outcome, SiteRules.CountDecision decision,
            Instant now) {
        String sql = "INSERT INTO site_count (count_id, event_id, site_id, sku, counted_quantity, counted_at, reason, outcome,"
                + " expected, later_movements, reserved, on_hand_before, on_hand_after, recorded_at)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setString(1, count.countId());
            s.setObject(2, eventId);
            s.setString(3, siteId);
            s.setString(4, count.sku());
            s.setInt(5, count.countedQuantity());
            s.setTimestamp(6, Timestamp.from(count.countedAt()));
            s.setString(7, count.reason());
            s.setString(8, outcome);
            if (decision == null) {
                for (int i = 9; i <= 13; i++) {
                    s.setNull(i, Types.INTEGER);
                }
            } else {
                s.setInt(9, Math.toIntExact(decision.expected()));
                s.setInt(10, Math.toIntExact(decision.laterMovements()));
                s.setInt(11, decision.reserved());
                s.setInt(12, decision.onHandBefore());
                s.setInt(13, decision.onHandAfter());
            }
            s.setTimestamp(14, Timestamp.from(now));
            s.executeUpdate();
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /** Record a hold transition. */
    @Transactional(Transactional.TxType.MANDATORY)
    public void recordHold(UUID eventId, String siteId, SiteEvent.Hold hold, String outcome, Instant now) {
        String sql = "INSERT INTO site_hold (hold_id, action, event_id, site_id, sku, quantity, reason_code, changed_at, outcome,"
                + " recorded_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setString(1, hold.holdId());
            s.setString(2, hold.action());
            s.setObject(3, eventId);
            s.setString(4, siteId);
            s.setString(5, hold.sku());
            s.setInt(6, hold.quantity());
            s.setString(7, hold.reasonCode());
            s.setTimestamp(8, Timestamp.from(hold.changedAt()));
            s.setString(9, outcome);
            s.setTimestamp(10, Timestamp.from(now));
            s.executeUpdate();
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /** Open a reconciliation item for a site event. A redelivered event opens nothing further. */
    @Transactional(Transactional.TxType.MANDATORY)
    public void reconciliation(String orderId, String reservationId, String kind, String siteId, String sku, String detailJson,
            UUID eventId, Instant now) {
        String sql = "INSERT INTO reconciliation_item (order_id, reservation_id, kind, site_id, sku, detail, event_id, created_at)"
                + " VALUES (?, ?, ?, ?, ?, CAST(? AS jsonb), ?, ?) ON CONFLICT (event_id, kind) DO NOTHING";
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setString(1, orderId);
            s.setString(2, reservationId);
            s.setString(3, kind);
            s.setString(4, siteId);
            s.setString(5, sku);
            s.setString(6, detailJson);
            s.setObject(7, eventId);
            s.setTimestamp(8, Timestamp.from(now));
            s.executeUpdate();
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /** Reconciliation items, newest first, optionally filtered. Includes the order-flow kinds as well as the site kinds. */
    public List<ReconciliationRow> reconciliationItems(String status, String kind, String siteId, String orderId, int limit,
            int offset) {
        String sql = "SELECT seq, kind, status, order_id, reservation_id, site_id, sku, shipment_id, stock_adjusted, event_id,"
                + " detail::text, created_at FROM reconciliation_item"
                + " WHERE (?::text IS NULL OR status = ?) AND (?::text IS NULL OR kind = ?)"
                + " AND (?::text IS NULL OR site_id = ?) AND (?::text IS NULL OR order_id = ?)"
                + " ORDER BY seq DESC LIMIT ? OFFSET ?";
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            int i = 1;
            for (String filter : new String[] {status, kind, siteId, orderId}) {
                s.setString(i++, filter);
                s.setString(i++, filter);
            }
            s.setInt(i++, limit);
            s.setInt(i, offset);
            List<ReconciliationRow> rows = new ArrayList<>();
            try (ResultSet r = s.executeQuery()) {
                while (r.next()) {
                    rows.add(new ReconciliationRow(r.getLong(1), r.getString(2), r.getString(3), r.getString(4), r.getString(5),
                            r.getString(6), r.getString(7), r.getString(8), r.getBoolean(9), r.getObject(10, UUID.class),
                            r.getString(11), r.getTimestamp(12).toInstant()));
                }
            }
            return rows;
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /** Recorded site decisions, newest first, optionally filtered by site and type (PICK, COUNT or HOLD). */
    public List<SiteDecisionRow> siteDecisions(String siteId, String type, int limit, int offset) {
        String sql = """
                SELECT * FROM (
                  SELECT 'PICK' AS type, pick_id AS id, NULL AS action, event_id, site_id, sku, quantity, order_id,
                         reservation_id, picked_at AS business_at, outcome, reservation_status, NULL::int AS expected,
                         NULL::int AS later_movements, NULL::int AS reserved, NULL::int AS on_hand_before,
                         NULL::int AS on_hand_after, NULL AS reason, recorded_at FROM site_pick
                  UNION ALL
                  SELECT 'COUNT', count_id, NULL, event_id, site_id, sku, counted_quantity, NULL, NULL, counted_at, outcome,
                         NULL, expected, later_movements, reserved, on_hand_before, on_hand_after, reason, recorded_at
                    FROM site_count
                  UNION ALL
                  SELECT 'HOLD', hold_id, action, event_id, site_id, sku, quantity, NULL, NULL, changed_at, outcome, NULL,
                         NULL, NULL, NULL, NULL, NULL, reason_code, recorded_at FROM site_hold
                ) d
                WHERE (?::text IS NULL OR d.site_id = ?) AND (?::text IS NULL OR d.type = ?)
                ORDER BY d.recorded_at DESC, d.id DESC, d.action NULLS FIRST LIMIT ? OFFSET ?
                """;
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setString(1, siteId);
            s.setString(2, siteId);
            s.setString(3, type);
            s.setString(4, type);
            s.setInt(5, limit);
            s.setInt(6, offset);
            List<SiteDecisionRow> rows = new ArrayList<>();
            try (ResultSet r = s.executeQuery()) {
                while (r.next()) {
                    rows.add(new SiteDecisionRow(r.getString(1), r.getString(2), r.getString(3), r.getObject(4, UUID.class),
                            r.getString(5), r.getString(6), r.getInt(7), r.getString(8), r.getString(9),
                            r.getTimestamp(10).toInstant(), r.getString(11), r.getString(12), integer(r, 13), integer(r, 14),
                            integer(r, 15), integer(r, 16), integer(r, 17), r.getString(18), r.getTimestamp(19).toInstant()));
                }
            }
            return rows;
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    private static Integer integer(ResultSet r, int column) throws SQLException {
        int value = r.getInt(column);
        return r.wasNull() ? null : value;
    }

    /** on_hand and reserved of a locked stock row. */
    public record StockRow(int onHand, int reserved) {}

    /** One reconciliation_item row. detailJson is the jsonb detail as text, or null. */
    public record ReconciliationRow(long seq, String kind, String status, String orderId, String reservationId, String siteId,
            String sku, String shipmentId, boolean stockAdjusted, UUID eventId, String detailJson, Instant createdAt) {}

    /** One recorded site decision: a pick, a count or a hold transition. */
    public record SiteDecisionRow(String type, String id, String action, UUID eventId, String siteId, String sku, int quantity,
            String orderId, String reservationId, Instant businessAt, String outcome, String reservationStatus, Integer expected,
            Integer laterMovements, Integer reserved, Integer onHandBefore, Integer onHandAfter, String reason,
            Instant recordedAt) {}
}
