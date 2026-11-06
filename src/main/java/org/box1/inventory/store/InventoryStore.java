package org.box1.inventory.store;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import javax.sql.DataSource;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.transaction.Transactional;
import org.box1.inventory.domain.ReasonCode;
import org.box1.inventory.domain.Reservation;
import org.box1.inventory.domain.ReservationStatus;
import org.box1.inventory.domain.StockLevel;

/** Inventory SQL. Writes join the caller's transaction, which also holds the dedupe record and the outbox row. */
@ApplicationScoped
public class InventoryStore {
    private static final String RESERVATION_COLUMNS = "reservation_id, order_id, site_id, sku, quantity, status, expires_at,"
            + " fulfillment_request_id, shipment_id, placed_event_id, correlation_id, traceparent, created_at, updated_at";

    private final DataSource dataSource;

    public InventoryStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /** Record an event as processed. False means it already was. */
    @Transactional(Transactional.TxType.MANDATORY)
    public boolean markProcessed(UUID eventId, String eventType, Instant now) {
        String sql = "INSERT INTO processed_event (event_id, event_type, processed_at) VALUES (?, ?, ?) ON CONFLICT DO NOTHING";
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setObject(1, eventId);
            s.setString(2, eventType);
            s.setTimestamp(3, Timestamp.from(now));
            return s.executeUpdate() == 1;
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /** The decision already made for an order, if any. */
    @Transactional(Transactional.TxType.MANDATORY)
    public Optional<Outcome> outcome(String orderId) {
        String sql = "SELECT outcome, reservation_id, reason_code, site_id, sku, quantity FROM order_outcome WHERE order_id = ?";
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setString(1, orderId);
            try (ResultSet r = s.executeQuery()) {
                if (!r.next()) {
                    return Optional.empty();
                }
                return Optional.of(new Outcome(r.getString(1), r.getString(2), r.getString(3), r.getString(4),
                        r.getString(5), r.getInt(6)));
            }
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /** Store the single decision for an order. A concurrent writer fails here on the primary key, and its retry reads this row. */
    @Transactional(Transactional.TxType.MANDATORY)
    public void recordOutcome(String orderId, Outcome outcome, UUID eventId, Instant now) {
        String sql = "INSERT INTO order_outcome (order_id, outcome, reservation_id, reason_code, site_id, sku, quantity, event_id, decided_at)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setString(1, orderId);
            s.setString(2, outcome.outcome());
            s.setString(3, outcome.reservationId());
            s.setString(4, outcome.reasonCode());
            s.setString(5, outcome.siteId());
            s.setString(6, outcome.sku());
            s.setInt(7, outcome.quantity());
            s.setObject(8, eventId);
            s.setTimestamp(9, Timestamp.from(now));
            s.executeUpdate();
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /**
     * Take a hold if stock allows. PostgreSQL locks the row, then re-checks the predicate against the committed row, so
     * two orders for the last unit serialize and the loser matches nothing.
     */
    @Transactional(Transactional.TxType.MANDATORY)
    public boolean tryHold(String siteId, String sku, int quantity, Instant now) {
        String sql = "UPDATE inventory_stock SET reserved = reserved + ?, updated_at = ?"
                + " WHERE site_id = ? AND sku = ? AND on_hand - reserved >= ?"
                + " AND EXISTS (SELECT 1 FROM inventory_site WHERE site_id = ? AND status = 'ACTIVE')";
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setInt(1, quantity);
            s.setTimestamp(2, Timestamp.from(now));
            s.setString(3, siteId);
            s.setString(4, sku);
            s.setInt(5, quantity);
            s.setString(6, siteId);
            return s.executeUpdate() == 1;
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /** Why a hold could not be taken. Runs only after tryHold matched nothing. */
    @Transactional(Transactional.TxType.MANDATORY)
    public ReasonCode classifyDenial(String siteId, String sku) {
        String sql = "SELECT (SELECT status FROM inventory_site WHERE site_id = ?),"
                + " EXISTS (SELECT 1 FROM inventory_stock WHERE site_id = ? AND sku = ?)";
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setString(1, siteId);
            s.setString(2, siteId);
            s.setString(3, sku);
            try (ResultSet r = s.executeQuery()) {
                r.next();
                if (!"ACTIVE".equals(r.getString(1))) {
                    return ReasonCode.SITE_UNAVAILABLE;
                }
                return r.getBoolean(2) ? ReasonCode.INSUFFICIENT_STOCK : ReasonCode.UNKNOWN_SKU;
            }
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }


    /** Insert a new hold in ACTIVE. */
    @Transactional(Transactional.TxType.MANDATORY)
    public void insertReservation(Reservation value) {
        String sql = "INSERT INTO reservations (" + RESERVATION_COLUMNS + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setString(1, value.reservationId());
            s.setString(2, value.orderId());
            s.setString(3, value.siteId());
            s.setString(4, value.sku());
            s.setInt(5, value.quantity());
            s.setString(6, value.status().name());
            s.setTimestamp(7, Timestamp.from(value.expiresAt()));
            s.setString(8, value.fulfillmentRequestId());
            s.setString(9, value.shipmentId());
            s.setObject(10, value.placedEventId());
            s.setString(11, value.correlationId());
            s.setString(12, value.traceparent());
            s.setTimestamp(13, Timestamp.from(value.createdAt()));
            s.setTimestamp(14, Timestamp.from(value.updatedAt()));
            s.executeUpdate();
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /** Read a reservation, outside or inside a transaction. */
    public Optional<Reservation> reservation(String reservationId) {
        return readOne("SELECT " + RESERVATION_COLUMNS + " FROM reservations WHERE reservation_id = ?", reservationId);
    }

    /** Read the single hold for an order. */
    public Optional<Reservation> reservationForOrder(String orderId) {
        return readOne("SELECT " + RESERVATION_COLUMNS + " FROM reservations WHERE order_id = ?", orderId);
    }

    private Optional<Reservation> readOne(String sql, String key) {
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setString(1, key);
            try (ResultSet r = s.executeQuery()) {
                return r.next() ? Optional.of(reservation(r)) : Optional.empty();
            }
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    private static Reservation reservation(ResultSet r) throws SQLException {
        return new Reservation(r.getString(1), r.getString(2), r.getString(3), r.getString(4), r.getInt(5),
                ReservationStatus.valueOf(r.getString(6)), r.getTimestamp(7).toInstant(), r.getString(8), r.getString(9),
                r.getObject(10, UUID.class), r.getString(11), r.getString(12), r.getTimestamp(13).toInstant(),
                r.getTimestamp(14).toInstant());
    }

    /** Move a hold from ACTIVE to COMMITTED. False if it was no longer ACTIVE. */
    @Transactional(Transactional.TxType.MANDATORY)
    public boolean commit(String reservationId, String fulfillmentRequestId, Instant now) {
        String sql = "UPDATE reservations SET status = 'COMMITTED', fulfillment_request_id = ?, updated_at = ?"
                + " WHERE reservation_id = ? AND status = 'ACTIVE'";
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setString(1, fulfillmentRequestId);
            s.setTimestamp(2, Timestamp.from(now));
            s.setString(3, reservationId);
            return s.executeUpdate() == 1;
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /** Expire a due hold. False if another path changed it first or the deadline has not passed. */
    @Transactional(Transactional.TxType.MANDATORY)
    public boolean expire(String reservationId, Instant now) {
        String sql = "UPDATE reservations SET status = 'EXPIRED', updated_at = ?"
                + " WHERE reservation_id = ? AND status = 'ACTIVE' AND expires_at <= ?";
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setTimestamp(1, Timestamp.from(now));
            s.setString(2, reservationId);
            s.setTimestamp(3, Timestamp.from(now));
            return s.executeUpdate() == 1;
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /** Release a hold from ACTIVE, or also from COMMITTED when the caller allows. False if nothing moved. */
    @Transactional(Transactional.TxType.MANDATORY)
    public boolean release(String reservationId, boolean includingCommitted, Instant now) {
        String sql = "UPDATE reservations SET status = 'RELEASED', updated_at = ? WHERE reservation_id = ? AND status "
                + (includingCommitted ? "IN ('ACTIVE','COMMITTED')" : "= 'ACTIVE'");
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setTimestamp(1, Timestamp.from(now));
            s.setString(2, reservationId);
            return s.executeUpdate() == 1;
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /** Mark a held reservation shipped. False if it was no longer held. */
    @Transactional(Transactional.TxType.MANDATORY)
    public boolean consume(String reservationId, String shipmentId, Instant now) {
        String sql = "UPDATE reservations SET status = 'CONSUMED', shipment_id = ?, updated_at = ?"
                + " WHERE reservation_id = ? AND status IN ('ACTIVE','COMMITTED')";
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setString(1, shipmentId);
            s.setTimestamp(2, Timestamp.from(now));
            s.setString(3, reservationId);
            return s.executeUpdate() == 1;
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /** Return held units to available. */
    @Transactional(Transactional.TxType.MANDATORY)
    public void returnHold(String siteId, String sku, int quantity, Instant now) {
        updateStock("UPDATE inventory_stock SET reserved = reserved - ?, updated_at = ? WHERE site_id = ? AND sku = ?",
                siteId, sku, quantity, now);
    }

    /** Take held units off the shelf: on hand and reserved fall together, so available is unchanged. */
    @Transactional(Transactional.TxType.MANDATORY)
    public void consumeHold(String siteId, String sku, int quantity, Instant now) {
        String sql = "UPDATE inventory_stock SET on_hand = on_hand - ?, reserved = reserved - ?, updated_at = ?"
                + " WHERE site_id = ? AND sku = ?";
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setInt(1, quantity);
            s.setInt(2, quantity);
            s.setTimestamp(3, Timestamp.from(now));
            s.setString(4, siteId);
            s.setString(5, sku);
            s.executeUpdate();
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /** Reduce on hand for a shipment with no hold, only if available covers it. */
    @Transactional(Transactional.TxType.MANDATORY)
    public boolean tryReduceOnHand(String siteId, String sku, int quantity, Instant now) {
        String sql = "UPDATE inventory_stock SET on_hand = on_hand - ?, updated_at = ?"
                + " WHERE site_id = ? AND sku = ? AND on_hand - reserved >= ?";
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setInt(1, quantity);
            s.setTimestamp(2, Timestamp.from(now));
            s.setString(3, siteId);
            s.setString(4, sku);
            s.setInt(5, quantity);
            return s.executeUpdate() == 1;
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    private void updateStock(String sql, String siteId, String sku, int quantity, Instant now) {
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setInt(1, quantity);
            s.setTimestamp(2, Timestamp.from(now));
            s.setString(3, siteId);
            s.setString(4, sku);
            s.executeUpdate();
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /** Append to the immutable transition history. */
    @Transactional(Transactional.TxType.MANDATORY)
    public void history(String reservationId, ReservationStatus from, ReservationStatus to, String actor, String reason,
            UUID eventId, Instant now) {
        String sql = "INSERT INTO reservation_history (reservation_id, from_status, to_status, actor, reason, event_id, occurred_at)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?)";
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setString(1, reservationId);
            s.setString(2, from == null ? null : from.name());
            s.setString(3, to.name());
            s.setString(4, actor);
            s.setString(5, reason);
            s.setObject(6, eventId);
            s.setTimestamp(7, Timestamp.from(now));
            s.executeUpdate();
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /** Append to the immutable on-hand ledger. */
    @Transactional(Transactional.TxType.MANDATORY)
    public void movement(String siteId, String sku, int delta, String kind, String actor, String reason, String reference,
            Instant now) {
        String sql = "INSERT INTO stock_movement (site_id, sku, delta, kind, actor, reason, reference, occurred_at)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setString(1, siteId);
            s.setString(2, sku);
            s.setInt(3, delta);
            s.setString(4, kind);
            s.setString(5, actor);
            s.setString(6, reason);
            s.setString(7, reference);
            s.setTimestamp(8, Timestamp.from(now));
            s.executeUpdate();
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /**
     * Restock under an idempotency key. The key row comes first, so a concurrent repeat waits for this transaction.
     * Returns false when the key already exists.
     */
    @Transactional(Transactional.TxType.MANDATORY)
    public boolean claimRestock(String siteId, String sku, int quantity, String actor, String reason, String key, String hash,
            Instant now) {
        String sql = "INSERT INTO stock_movement (site_id, sku, delta, kind, actor, reason, idempotency_key, request_hash, occurred_at)"
                + " VALUES (?, ?, ?, 'RESTOCK', ?, ?, ?, ?, ?) ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING";
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setString(1, siteId);
            s.setString(2, sku);
            s.setInt(3, quantity);
            s.setString(4, actor);
            s.setString(5, reason);
            s.setString(6, key);
            s.setString(7, hash);
            s.setTimestamp(8, Timestamp.from(now));
            return s.executeUpdate() == 1;
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /** The request hash stored for a restock key. */
    @Transactional(Transactional.TxType.MANDATORY)
    public String restockHash(String key) {
        String sql = "SELECT request_hash FROM stock_movement WHERE idempotency_key = ?";
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setString(1, key);
            try (ResultSet r = s.executeQuery()) {
                r.next();
                return r.getString(1);
            }
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /** Create a site. False means the site already exists, and nothing was changed. */
    @Transactional(Transactional.TxType.MANDATORY)
    public boolean createSite(String siteId, String status, String actor, String reason, Instant now) {
        try (Connection c = dataSource.getConnection();
                PreparedStatement s = c.prepareStatement(
                        "INSERT INTO inventory_site (site_id, status) VALUES (?, ?) ON CONFLICT (site_id) DO NOTHING")) {
            s.setString(1, siteId);
            s.setString(2, status);
            if (s.executeUpdate() == 0) {
                return false;
            }
        } catch (SQLException error) {
            throw new StoreException(error);
        }
        String sql = "INSERT INTO site_registration (site_id, status, actor, reason, occurred_at) VALUES (?, ?, ?, ?, ?)";
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setString(1, siteId);
            s.setString(2, status);
            s.setString(3, actor);
            s.setString(4, reason);
            s.setTimestamp(5, Timestamp.from(now));
            s.executeUpdate();
            return true;
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /** A site's status, if the site exists. */
    @Transactional(Transactional.TxType.MANDATORY)
    public Optional<String> siteStatus(String siteId) {
        try (Connection c = dataSource.getConnection();
                PreparedStatement s = c.prepareStatement("SELECT status FROM inventory_site WHERE site_id = ?")) {
            s.setString(1, siteId);
            try (ResultSet r = s.executeQuery()) {
                return r.next() ? Optional.of(r.getString(1)) : Optional.empty();
            }
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /** Create a stock record with its opening quantity. False means the record already exists, and nothing was changed. */
    @Transactional(Transactional.TxType.MANDATORY)
    public boolean createStock(String siteId, String sku, int openingQuantity, String actor, String reason, Instant now) {
        String sql = "INSERT INTO inventory_stock (site_id, sku, on_hand, updated_at) VALUES (?, ?, ?, ?)"
                + " ON CONFLICT (site_id, sku) DO NOTHING";
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setString(1, siteId);
            s.setString(2, sku);
            s.setInt(3, openingQuantity);
            s.setTimestamp(4, Timestamp.from(now));
            if (s.executeUpdate() == 0) {
                return false;
            }
        } catch (SQLException error) {
            throw new StoreException(error);
        }
        movement(siteId, sku, openingQuantity, "OPENING", actor, reason, null, now);
        return true;
    }

    /** The opening quantity of a stock record created through the API; empty for seeded records. */
    @Transactional(Transactional.TxType.MANDATORY)
    public Optional<Integer> openingQuantity(String siteId, String sku) {
        String sql = "SELECT delta FROM stock_movement WHERE site_id = ? AND sku = ? AND kind = 'OPENING'";
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setString(1, siteId);
            s.setString(2, sku);
            try (ResultSet r = s.executeQuery()) {
                return r.next() ? Optional.of(r.getInt(1)) : Optional.empty();
            }
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /** Add units to on hand. */
    @Transactional(Transactional.TxType.MANDATORY)
    public void addOnHand(String siteId, String sku, int quantity, Instant now) {
        updateStock("UPDATE inventory_stock SET on_hand = on_hand + ?, updated_at = ? WHERE site_id = ? AND sku = ?",
                siteId, sku, quantity, now);
    }

    /** Open a reconciliation item. A redelivered event opens nothing further. */
    @Transactional(Transactional.TxType.MANDATORY)
    public void reconciliation(String orderId, String reservationId, String kind, boolean stockAdjusted, String shipmentId,
            UUID eventId, Instant now) {
        String sql = "INSERT INTO reconciliation_item (order_id, reservation_id, kind, stock_adjusted, shipment_id, event_id, created_at)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (event_id, kind) DO NOTHING";
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setString(1, orderId);
            s.setString(2, reservationId);
            s.setString(3, kind);
            s.setBoolean(4, stockAdjusted);
            s.setString(5, shipmentId);
            s.setObject(6, eventId);
            s.setTimestamp(7, Timestamp.from(now));
            s.executeUpdate();
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /** Whether a late shipment was already recorded for this hold: this one, or any that adjusted stock. */
    @Transactional(Transactional.TxType.MANDATORY)
    public LateShipment lateShipment(String reservationId, String shipmentId) {
        String sql = "SELECT COALESCE(bool_or(shipment_id = ?), false), COALESCE(bool_or(stock_adjusted), false)"
                + " FROM reconciliation_item WHERE reservation_id = ? AND kind = 'SHIPMENT_AFTER_RELEASE'";
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setString(1, shipmentId);
            s.setString(2, reservationId);
            try (ResultSet r = s.executeQuery()) {
                r.next();
                return new LateShipment(r.getBoolean(1), r.getBoolean(2));
            }
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /** Read a hold and lock it, so what the caller then records as its previous status is true. */
    @Transactional(Transactional.TxType.MANDATORY)
    public Optional<Reservation> lockForOrder(String orderId) {
        return readOne("SELECT " + RESERVATION_COLUMNS + " FROM reservations WHERE order_id = ? FOR UPDATE", orderId);
    }

    /** Read a hold by ID and lock it. */
    @Transactional(Transactional.TxType.MANDATORY)
    public Optional<Reservation> lock(String reservationId) {
        return readOne("SELECT " + RESERVATION_COLUMNS + " FROM reservations WHERE reservation_id = ? FOR UPDATE", reservationId);
    }

    /** IDs of holds past their deadline, oldest first. */
    public List<String> dueReservations(Instant now, int limit) {
        String sql = "SELECT reservation_id FROM reservations WHERE status = 'ACTIVE' AND expires_at <= ? ORDER BY expires_at LIMIT ?";
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setTimestamp(1, Timestamp.from(now));
            s.setInt(2, limit);
            List<String> ids = new ArrayList<>();
            try (ResultSet r = s.executeQuery()) {
                while (r.next()) {
                    ids.add(r.getString(1));
                }
            }
            return ids;
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /** Stock levels, optionally filtered, in a stable order. */
    public List<StockLevel> stock(String siteId, String sku, int limit, int offset) {
        String sql = "SELECT site_id, sku, on_hand, reserved, updated_at FROM inventory_stock"
                + " WHERE (?::text IS NULL OR site_id = ?) AND (?::text IS NULL OR sku = ?)"
                + " ORDER BY site_id, sku LIMIT ? OFFSET ?";
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setString(1, siteId);
            s.setString(2, siteId);
            s.setString(3, sku);
            s.setString(4, sku);
            s.setInt(5, limit);
            s.setInt(6, offset);
            List<StockLevel> levels = new ArrayList<>();
            try (ResultSet r = s.executeQuery()) {
                while (r.next()) {
                    levels.add(new StockLevel(r.getString(1), r.getString(2), r.getInt(3), r.getInt(4), r.getTimestamp(5).toInstant()));
                }
            }
            return levels;
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /** Values behind the business gauges, in one query. */
    public Stats stats() {
        String sql = """
                SELECT (SELECT count(*) FROM outbox WHERE published_at IS NULL),
                       (SELECT COALESCE(EXTRACT(EPOCH FROM now() - min(created_at)), 0) FROM outbox WHERE published_at IS NULL),
                       (SELECT count(*) FROM reservations WHERE status = 'ACTIVE'),
                       (SELECT count(*) FROM reservations WHERE status = 'COMMITTED'),
                       (SELECT COALESCE(EXTRACT(EPOCH FROM now() - min(updated_at)), 0) FROM reservations WHERE status = 'COMMITTED'),
                       (SELECT count(*) FROM reconciliation_item WHERE status = 'OPEN'),
                       (SELECT count(*) FROM inventory_stock t WHERE t.reserved <> COALESCE(
                            (SELECT sum(r.quantity) FROM reservations r WHERE r.site_id = t.site_id AND r.sku = t.sku
                             AND r.status IN ('ACTIVE','COMMITTED')), 0))
                """;
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql);
                ResultSet r = s.executeQuery()) {
            r.next();
            return new Stats(r.getLong(1), r.getDouble(2), r.getLong(3), r.getLong(4), r.getDouble(5), r.getLong(6), r.getLong(7));
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /** What is already recorded about late shipments for a hold. */
    public record LateShipment(boolean sameShipment, boolean adjusted) {}

    /** A stored order decision. */
    public record Outcome(String outcome, String reservationId, String reasonCode, String siteId, String sku, int quantity) {}

    /** Values behind the business gauges. */
    public record Stats(long outboxPending, double oldestPendingSeconds, long active, long committed,
            double oldestCommittedSeconds, long reconciliationOpen, long reservedDrift) {}
}
