package org.box1.inventory;

import java.time.Clock;
import java.time.Instant;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/** Refuses a floor in the future, which would silently acknowledge real orders and commit past them. */
@ApplicationScoped
public class StartupChecks {
    void check(@Observes StartupEvent event, Clock clock,
            @ConfigProperty(name = "inventory.consumer.order-events-floor") Instant floor) {
        if (floor.isAfter(clock.instant())) {
            throw new IllegalStateException("inventory.consumer.order-events-floor is in the future");
        }
    }
}
