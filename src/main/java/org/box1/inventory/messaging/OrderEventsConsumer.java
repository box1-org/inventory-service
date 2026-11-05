package org.box1.inventory.messaging;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.smallrye.common.annotation.Blocking;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.box1.inventory.InventoryMetrics;
import org.box1.inventory.ReservationService;
import org.box1.inventory.domain.InboundEvent;
import org.box1.inventory.domain.PoisonEventException;
import org.box1.inventory.store.StoreException;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;

/**
 * Consumes order and fulfillment events. A thrown exception nacks the record, and the connector's dead-letter strategy
 * moves it to the dead-letter topic. Offsets commit only after processing.
 */
@ApplicationScoped
public class OrderEventsConsumer {
    private static final Logger LOG = Logger.getLogger(OrderEventsConsumer.class);
    private static final TextMapGetter<Map<String, String>> GETTER = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(Map<String, String> carrier) {
            return carrier.keySet();
        }

        @Override
        public String get(Map<String, String> carrier, String key) {
            return carrier == null ? null : carrier.get(key);
        }
    };

    private final EventParser parser;
    private final ReservationService reservations;
    private final InventoryMetrics metrics;
    private final Tracer tracer;
    private final Clock clock;
    private final Instant floor;
    private final int maxAttempts;
    private final long initialBackoffMillis;
    private final long maxBackoffMillis;

    public OrderEventsConsumer(EventParser parser, ReservationService reservations, InventoryMetrics metrics, Tracer tracer,
            Clock clock, @ConfigProperty(name = "inventory.consumer.order-events-floor") String floor,
            @ConfigProperty(name = "inventory.consumer.max-attempts") int maxAttempts,
            @ConfigProperty(name = "inventory.consumer.initial-backoff-millis") long initialBackoffMillis,
            @ConfigProperty(name = "inventory.consumer.max-backoff-millis") long maxBackoffMillis) {
        this.parser = parser;
        this.reservations = reservations;
        this.metrics = metrics;
        this.tracer = tracer;
        this.clock = clock;
        this.floor = parseFloor(floor);
        this.maxAttempts = maxAttempts;
        this.initialBackoffMillis = initialBackoffMillis;
        this.maxBackoffMillis = maxBackoffMillis;
    }

    private static Instant parseFloor(String value) {
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException error) {
            throw new IllegalStateException("INVENTORY_ORDER_EVENTS_FLOOR must be an ISO-8601 instant", error);
        }
    }

    @Incoming("order-events")
    @Blocking
    void onOrderEvent(ConsumerRecord<String, byte[]> record) {
        process(text(record), EventParser.ORDER_TOPIC_TYPES);
    }

    @Incoming("fulfillment-events")
    @Blocking
    void onFulfillmentEvent(ConsumerRecord<String, byte[]> record) {
        process(text(record), EventParser.FULFILLMENT_TOPIC_TYPES);
    }

    /** Bytes stay raw on the channel, so a dead-lettered record is exactly what arrived. */
    private static String text(ConsumerRecord<String, byte[]> record) {
        return record.value() == null ? null : new String(record.value(), StandardCharsets.UTF_8);
    }

    private void process(String value, Set<String> allowedTypes) {
        InboundEvent event;
        try {
            event = parser.parse(value, allowedTypes);
        } catch (PoisonEventException error) {
            metrics.consumed("unparsed", "dead_lettered");
            LOG.warnv("Dead-lettering malformed event: {0}", error.getMessage());
            throw error;
        }
        Span span = tracer.spanBuilder("inventory.consume " + event.eventType()).setParent(extract(event.traceparent())).startSpan();
        MDC.put("orderId", event.orderId());
        MDC.put("eventId", event.eventId().toString());
        MDC.put("eventType", event.eventType());
        MDC.put("correlationId", event.correlationId());
        try (Scope ignored = span.makeCurrent()) {
            metrics.eventAge(Duration.between(event.occurredAt(), clock.instant()).toMillis() / 1000.0);
            if (event.occurredAt().isBefore(floor)) {
                // Acknowledged with no effect and no dedupe record. See the offset policy in docs/runbook.md.
                metrics.consumed(event.eventType(), "skipped_before_floor");
                LOG.warn("Skipping an event older than the configured floor");
                return;
            }
            ReservationService.Applied applied = withRetries(reservations::apply, event);
            if (applied.units() > 0) {
                metrics.reserved(applied.siteId(), applied.units());
            }
            if (applied.reason() != null) {
                metrics.rejected(applied.reason());
            }
            metrics.consumed(event.eventType(), applied.outcome());
            LOG.infov("Event {0}", applied.outcome());
        } catch (RuntimeException error) {
            span.recordException(error);
            metrics.consumed(event.eventType(), "dead_lettered");
            LOG.warnv("Dead-lettering event: {0}", error.getClass().getSimpleName());
            if (error instanceof PoisonEventException) {
                throw error;
            }
            // Fixed text: this message and any cause reach dead-letter headers.
            throw new PoisonEventException("Retries exhausted");
        } finally {
            span.end();
            MDC.remove("orderId");
            MDC.remove("eventId");
            MDC.remove("eventType");
            MDC.remove("correlationId");
        }
    }

    private <T, R> R withRetries(Function<T, R> action, T input) {
        long backoff = initialBackoffMillis;
        for (int attempt = 1; ; attempt++) {
            try {
                return action.apply(input);
            } catch (PoisonEventException error) {
                throw error;
            } catch (RuntimeException error) {
                if (attempt >= maxAttempts) {
                    throw error;
                }
                metrics.retried();
                if (error instanceof StoreException store && store.isConflict()) {
                    metrics.conflictRetry();
                }
                LOG.warnv("Retrying event: attempt={0} error={1}", attempt, error.getClass().getSimpleName());
                sleep(backoff);
                backoff = Math.min(backoff * 2, maxBackoffMillis);
            }
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted during event retry", error);
        }
    }

    private static Context extract(String traceparent) {
        if (traceparent == null) {
            return Context.current();
        }
        return W3CTraceContextPropagator.getInstance().extract(Context.current(), Map.of("traceparent", traceparent), GETTER);
    }
}
