package org.box1.inventory;

import java.time.Clock;

import io.quarkus.arc.DefaultBean;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

/** The only application time source. Tests replace it, which makes expiry deterministic. */
@ApplicationScoped
public class ClockProducer {
    @Produces
    @DefaultBean
    @ApplicationScoped
    Clock clock() {
        return Clock.systemUTC();
    }
}
