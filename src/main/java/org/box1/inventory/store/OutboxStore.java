package org.box1.inventory.store;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import javax.sql.DataSource;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.transaction.Transactional;

/** Outbox SQL. Each publisher step is its own transaction, separate from any broker call. */
@ApplicationScoped
public class OutboxStore {
    /** Claims at most one event per order, the oldest, so a retry cannot overtake it. */
    private static final String CLAIM = """
            WITH candidates AS (
                SELECT o.event_id FROM outbox o
                WHERE o.published_at IS NULL
                  AND o.next_attempt_at <= now()
                  AND (o.claim_expires_at IS NULL OR o.claim_expires_at < now())
                  AND NOT EXISTS (SELECT 1 FROM outbox e
                                  WHERE e.aggregate_id = o.aggregate_id AND e.seq < o.seq AND e.published_at IS NULL)
                ORDER BY o.seq
                LIMIT ?
                FOR UPDATE OF o SKIP LOCKED
            )
            UPDATE outbox u
            SET claimed_by = ?, claim_expires_at = now() + make_interval(secs => ?), attempts = u.attempts + 1
            FROM candidates c
            WHERE u.event_id = c.event_id
            RETURNING u.seq, u.event_id, u.aggregate_id, u.event_type, u.payload::text, u.attempts
            """;

    private final DataSource dataSource;

    public OutboxStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /** Enqueue an event in the caller's business transaction. */
    @Transactional(Transactional.TxType.MANDATORY)
    public void enqueue(UUID eventId, String aggregateId, String eventType, String envelopeJson) {
        // Database time: claims compare next_attempt_at with now(), so an application clock must not decide this.
        String sql = "INSERT INTO outbox (event_id, aggregate_id, event_type, payload, created_at, next_attempt_at)"
                + " VALUES (?, ?, ?, ?::jsonb, now(), now())";
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setObject(1, eventId);
            s.setString(2, aggregateId);
            s.setString(3, eventType);
            s.setString(4, envelopeJson);
            s.executeUpdate();
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /** Claim a bounded batch under a lease. A crashed claimer's lease expires and the rows become claimable. */
    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public List<Claimed> claim(String owner, int limit, double leaseSeconds) {
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(CLAIM)) {
            s.setInt(1, limit);
            s.setString(2, owner);
            s.setDouble(3, leaseSeconds);
            List<Claimed> claimed = new ArrayList<>();
            try (ResultSet r = s.executeQuery()) {
                while (r.next()) {
                    claimed.add(new Claimed(r.getLong(1), r.getObject(2, UUID.class), r.getString(3), r.getString(4),
                            r.getString(5), r.getInt(6)));
                }
            }
            claimed.sort(Comparator.comparingLong(Claimed::seq));
            return claimed;
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /** Mark published after broker confirmation. Returns the seconds the event waited, or -1 if the claim was lost. */
    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public double markPublished(UUID eventId, String owner) {
        String sql = "UPDATE outbox SET published_at = now(), claimed_by = NULL, claim_expires_at = NULL, last_error = NULL"
                + " WHERE event_id = ? AND claimed_by = ? AND published_at IS NULL"
                + " RETURNING EXTRACT(EPOCH FROM now() - created_at)";
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setObject(1, eventId);
            s.setString(2, owner);
            try (ResultSet r = s.executeQuery()) {
                return r.next() ? r.getDouble(1) : -1;
            }
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /** Release a failed claim and schedule the retry. */
    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public void release(UUID eventId, String owner, double delaySeconds, String errorType) {
        String sql = "UPDATE outbox SET claimed_by = NULL, claim_expires_at = NULL, last_error = ?,"
                + " next_attempt_at = now() + make_interval(secs => ?) WHERE event_id = ? AND claimed_by = ?";
        try (Connection c = dataSource.getConnection(); PreparedStatement s = c.prepareStatement(sql)) {
            s.setString(1, errorType);
            s.setDouble(2, delaySeconds);
            s.setObject(3, eventId);
            s.setString(4, owner);
            s.executeUpdate();
        } catch (SQLException error) {
            throw new StoreException(error);
        }
    }

    /** A claimed outbox row. */
    public record Claimed(long seq, UUID eventId, String aggregateId, String eventType, String envelopeJson, int attempts) {}
}
