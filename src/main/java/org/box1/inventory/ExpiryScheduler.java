package org.box1.inventory;

import java.time.Clock;
import java.util.List;
import java.util.Optional;

import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import org.box1.inventory.domain.Reservation;
import org.box1.inventory.store.InventoryStore;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;

/**
 * Finds due holds and expires each in its own transaction. Re-running, crashing or a second pod during a rollout is safe,
 * because expiry is one conditional update and only one caller can win it. Polling waits for {@link ExpiryGate}, so that
 * recovery from an outage cannot expire a hold whose fulfillment signal is still on the topic.
 */
@ApplicationScoped
public class ExpiryScheduler {
    private static final Logger LOG = Logger.getLogger(ExpiryScheduler.class);

    private final InventoryStore store;
    private final ReservationService reservations;
    private final InventoryMetrics metrics;
    private final Clock clock;
    private final ExpiryGate gate;
    private final int batchSize;

    public ExpiryScheduler(InventoryStore store, ReservationService reservations, InventoryMetrics metrics, Clock clock,
            ExpiryGate gate, @ConfigProperty(name = "inventory.expiry.batch-size") int batchSize) {
        this.store = store;
        this.reservations = reservations;
        this.metrics = metrics;
        this.clock = clock;
        this.gate = gate;
        this.batchSize = batchSize;
    }

    @Scheduled(every = "${inventory.expiry.interval}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    void poll() {
        try {
            if (!gate.open()) {
                metrics.expiryHeld();
                return;
            }
            expireDue();
        } catch (RuntimeException error) {
            LOG.warnv("Expiry poll failed: {0}", error.getClass().getSimpleName());
        }
    }

    /** Expire one bounded batch of due holds. Returns how many this call expired. */
    public int expireDue() {
        List<String> due = store.dueReservations(clock.instant(), batchSize);
        int expired = 0;
        for (String reservationId : due) {
            try {
                Optional<Reservation> hold = reservations.expire(reservationId);
                if (hold.isPresent()) {
                    expired++;
                    metrics.expired();
                    logExpired(hold.get());
                }
            } catch (RuntimeException error) {
                LOG.warnv("Unable to expire a reservation: reservationId={0} error={1}", reservationId,
                        error.getClass().getSimpleName());
            }
        }
        return expired;
    }

    private static void logExpired(Reservation hold) {
        MDC.put("orderId", hold.orderId());
        MDC.put("correlationId", hold.correlationId());
        try {
            LOG.infov("Reservation expired: reservationId={0}", hold.reservationId());
        } finally {
            MDC.remove("orderId");
            MDC.remove("correlationId");
        }
    }
}
