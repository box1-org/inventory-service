package org.box1.inventory;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

import org.junit.jupiter.api.Test;

class ExpiryGateTest {
    private static final Duration GRACE = Duration.ofMinutes(2);

    private static final class Manual extends Clock {
        private Instant now = Instant.parse("2026-11-05T12:00:00Z");

        void advance(Duration by) {
            now = now.plus(by);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }

    private final Manual clock = new Manual();

    private ExpiryGate open() {
        ExpiryGate gate = new ExpiryGate(clock, GRACE, Duration.ofSeconds(30));
        clock.advance(GRACE);
        gate.brokerReached();
        return gate;
    }

    @Test
    void closedUntilTheStartupGraceEnds() {
        ExpiryGate gate = new ExpiryGate(clock, GRACE, Duration.ofSeconds(30));
        assertFalse(gate.open());
        clock.advance(GRACE);
        gate.brokerReached();
        assertTrue(gate.open());
    }

    @Test
    void closedDuringRetriesAndForAGraceAfter() {
        ExpiryGate gate = open();
        gate.retrying();
        clock.advance(Duration.ofHours(1));
        gate.brokerReached();
        assertFalse(gate.open(), "however long the outage lasts");
        gate.retriesEnded();
        assertFalse(gate.open(), "the consumer now has a backlog to read");
        clock.advance(GRACE);
        gate.brokerReached();
        assertTrue(gate.open());
    }

    @Test
    void aLateEventClosesItAndAFreshOneDoesNot() {
        ExpiryGate gate = open();
        gate.observedEventAge(1);
        assertTrue(gate.open());
        gate.observedEventAge(31);
        assertFalse(gate.open());
        clock.advance(GRACE);
        gate.brokerReached();
        assertTrue(gate.open());
    }

    @Test
    void anUnreachableBrokerClosesIt() {
        ExpiryGate gate = open();
        clock.advance(GRACE.plusSeconds(1));
        assertFalse(gate.open());
        gate.brokerReached();
        assertTrue(gate.open());
    }

    @Test
    void noGraceLeavesOnlyActiveRetries() {
        ExpiryGate gate = new ExpiryGate(clock, Duration.ZERO, Duration.ofSeconds(30));
        assertTrue(gate.open());
        clock.advance(Duration.ofDays(1));
        assertTrue(gate.open());
        gate.retrying();
        assertFalse(gate.open());
        gate.retriesEnded();
        assertTrue(gate.open());
    }
}
