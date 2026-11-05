package org.box1.inventory.messaging;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
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
import org.box1.inventory.ExpiryGate;
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
    private final ExpiryGate gate;
    private final int maxAttempts;
    private final long initialBackoffMillis;
    private final long maxBackoffMillis;

    public OrderEventsConsumer(EventParser parser, ReservationService reservations, InventoryMetrics metrics, Tracer tracer,
            Clock clock, ExpiryGate gate, @ConfigProperty(name = "inventory.consumer.order-events-floor") Instant floor,
            @ConfigProperty(name = "inventory.consumer.max-attempts") int maxAttempts,
            @ConfigProperty(name = "inventory.consumer.initial-backoff-millis") long initialBackoffMillis,
            @ConfigProperty(name = "inventory.consumer.max-backoff-millis") long maxBackoffMillis) {
        this.parser = parser;
        this.reservations = reservations;
        this.metrics = metrics;
        this.tracer = tracer;
        this.clock = clock;
        this.floor = floor;
        this.gate = gate;
        this.maxAttempts = maxAttempts;
        this.initialBackoffMillis = initialBackoffMillis;
        this.maxBackoffMillis = maxBackoffMillis;
    }

    @Incoming("order-events")
    @Blocking
    void onOrderEvent(ConsumerRecord<String, byte[]> record) {
        process(record, EventParser.ORDER_TOPIC_TYPES);
    }

    @Incoming("fulfillment-events")
    @Blocking
    void onFulfillmentEvent(ConsumerRecord<String, byte[]> record) {
        process(record, EventParser.FULFILLMENT_TOPIC_TYPES);
    }

    /** Bytes stay raw on the channel, so a dead-lettered record is exactly what arrived. Invalid UTF-8 is poison. */
    private static String text(ConsumerRecord<String, byte[]> record) {
        if (record.value() == null) {
            return null;
        }
        try {
            return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(record.value())).toString();
        } catch (CharacterCodingException error) {
            throw new PoisonEventException("Record value is not valid UTF-8");
        }
    }

    private void process(ConsumerRecord<String, byte[]> record, Set<String> allowedTypes) {
        InboundEvent event;
        try {
            event = parser.parse(text(record), allowedTypes);
        } catch (PoisonEventException error) {
            metrics.consumed("unparsed", "dead_lettered");
            LOG.warnv("Dead-lettering malformed event: topic={0} partition={1} offset={2} reason={3}", record.topic(),
                    record.partition(), record.offset(), error.getMessage());
            throw error;
        }
        Span span = tracer.spanBuilder("inventory.consume " + event.eventType()).setParent(extract(event.traceparent()))
                .setAttribute("box1.order_id", event.orderId()).startSpan();
        MDC.put("orderId", event.orderId());
        MDC.put("eventId", event.eventId().toString());
        MDC.put("eventType", event.eventType());
        MDC.put("correlationId", event.correlationId());
        try (Scope ignored = span.makeCurrent()) {
            if (event.occurredAt().isBefore(floor)) {
                // Acknowledged with no effect and no dedupe record. See the offset policy in docs/runbook.md.
                metrics.consumed(event.eventType(), "skipped_before_floor");
                LOG.warn("Skipping an event older than the configured floor");
                return;
            }
            double age = Duration.between(event.occurredAt(), clock.instant()).toMillis() / 1000.0;
            metrics.eventAge(age);
            gate.observedEventAge(age);
            try {
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
                LOG.warnv("Dead-lettering event: topic={0} partition={1} offset={2} error={3}", record.topic(), record.partition(),
                        record.offset(), error.getClass().getSimpleName());
                if (error instanceof PoisonEventException) {
                    throw error;
                }
                // Fixed text: this message and any cause reach dead-letter headers.
                throw new PoisonEventException("Retries exhausted");
            }
        } finally {
            span.end();
            MDC.remove("orderId");
            MDC.remove("eventId");
            MDC.remove("eventType");
            MDC.remove("correlationId");
        }
    }

    /**
     * Retries until the event applies. A database that cannot serve holds the channel without using up attempts, because
     * no event is at fault and dead-lettering a good one would cost a healthy order. Everything else stays bounded.
     */
    private <T, R> R withRetries(Function<T, R> action, T input) {
        long backoff = initialBackoffMillis;
        int attempt = 1;
        boolean retried = false;
        try {
            while (true) {
                try {
                    return action.apply(input);
                } catch (PoisonEventException error) {
                    throw error;
                } catch (StoreException error) {
                    if (error.isPermanent()) {
                        throw new PoisonEventException("Value cannot be stored");
                    }
                    boolean holdsPosition = error.isUnavailable();
                    if (!holdsPosition && attempt >= maxAttempts) {
                        throw error;
                    }
                    if (error.isConflict()) {
                        metrics.conflictRetry();
                    }
                    if (holdsPosition) {
                        metrics.heldForDatabase();
                    } else {
                        attempt++;
                    }
                    retried = enterRetries(retried);
                    backoff = retryPause(backoff, attempt, error);
                } catch (RuntimeException error) {
                    if (attempt >= maxAttempts) {
                        throw error;
                    }
                    attempt++;
                    retried = enterRetries(retried);
                    backoff = retryPause(backoff, attempt, error);
                }
            }
        } finally {
            if (retried) {
                gate.retriesEnded();
            }
        }
    }

    private boolean enterRetries(boolean already) {
        if (!already) {
            gate.retrying();
        }
        return true;
    }

    private long retryPause(long backoff, int attempt, RuntimeException error) {
        metrics.retried();
        LOG.warnv("Retrying event: attempt={0} error={1}", attempt, error.getClass().getSimpleName());
        sleep(backoff);
        return Math.min(backoff * 2, maxBackoffMillis);
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
