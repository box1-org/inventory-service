package org.box1.inventory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Decides whether holds may expire now. Order turns an expiry into EXCEPTION, so expiring while events may be unread could
 * turn a healthy order into an exception: its fulfillment signal may be on the topic, waiting. After startup, a consumer
 * retry, an event arriving late, or an unreachable broker, expiry waits a grace period. A late expiry only holds stock
 * longer, which is the safe side.
 */
@ApplicationScoped
public class ExpiryGate {
    private final Clock clock;
    private final Duration grace;
    private final Duration behindThreshold;
    private final AtomicInteger retrying = new AtomicInteger();
    private final AtomicReference<Instant> notBefore;
    private final AtomicReference<Instant> brokerReached;

    public ExpiryGate(Clock clock, @ConfigProperty(name = "inventory.expiry.grace") Duration grace,
            @ConfigProperty(name = "inventory.expiry.behind-threshold") Duration behindThreshold) {
        this.clock = clock;
        this.grace = grace;
        this.behindThreshold = behindThreshold;
        this.notBefore = new AtomicReference<>(clock.instant().plus(grace));
        this.brokerReached = new AtomicReference<>(clock.instant());
    }

    /** True when due holds may expire. With no grace configured, only an active retry closes it. */
    public boolean open() {
        Instant now = clock.instant();
        if (retrying.get() > 0 || now.isBefore(notBefore.get())) {
            return false;
        }
        return grace.isZero() || !now.isAfter(brokerReached.get().plus(grace));
    }

    /** A consumer channel has started retrying an event. */
    public void retrying() {
        retrying.incrementAndGet();
        hold();
    }

    /** That channel has finished with the event, either way. It may now have a backlog to read. */
    public void retriesEnded() {
        retrying.decrementAndGet();
        hold();
    }

    /** An event arrived. If it is old, the consumer is catching up. */
    public void observedEventAge(double seconds) {
        if (seconds > behindThreshold.toSeconds()) {
            hold();
        }
    }

    /** The broker answered. */
    public void brokerReached() {
        brokerReached.set(clock.instant());
    }

    private void hold() {
        Instant until = clock.instant().plus(grace);
        notBefore.getAndUpdate(current -> until.isAfter(current) ? until : current);
    }
}
