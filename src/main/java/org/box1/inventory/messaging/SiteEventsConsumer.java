package org.box1.inventory.messaging;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.smallrye.common.annotation.Blocking;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.box1.inventory.InventoryMetrics;
import org.box1.inventory.SiteEventService;
import org.box1.inventory.domain.PoisonEventException;
import org.box1.inventory.domain.SiteEvent;
import org.box1.inventory.store.StoreException;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;

/**
 * Consumes box1.site.events.v1 as group inventory-site. A thrown exception nacks the record, and the connector's
 * dead-letter strategy moves it, unchanged, to box1.site.dlq.v1 with reason headers. Offsets commit only after
 * processing. A database that cannot serve holds the record without using attempts, as for the order consumers. Unlike
 * them, retries here do not hold reservation expiry: site events never touch reservations.
 */
@ApplicationScoped
public class SiteEventsConsumer {
    static final String SITE_HEADER = "box1-site-id";
    private static final Logger LOG = Logger.getLogger(SiteEventsConsumer.class);
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

    private final SiteEventParser parser;
    private final SiteEventService sites;
    private final InventoryMetrics metrics;
    private final Tracer tracer;
    private final int maxAttempts;
    private final long initialBackoffMillis;
    private final long maxBackoffMillis;

    public SiteEventsConsumer(SiteEventParser parser, SiteEventService sites, InventoryMetrics metrics, Tracer tracer,
            @ConfigProperty(name = "inventory.consumer.max-attempts") int maxAttempts,
            @ConfigProperty(name = "inventory.consumer.initial-backoff-millis") long initialBackoffMillis,
            @ConfigProperty(name = "inventory.consumer.max-backoff-millis") long maxBackoffMillis) {
        this.parser = parser;
        this.sites = sites;
        this.metrics = metrics;
        this.tracer = tracer;
        this.maxAttempts = maxAttempts;
        this.initialBackoffMillis = initialBackoffMillis;
        this.maxBackoffMillis = maxBackoffMillis;
    }

    @Incoming("site-events")
    @Blocking
    void onSiteEvent(ConsumerRecord<String, byte[]> record) {
        Optional<SiteEvent> parsed;
        try {
            parsed = parser.parse(text(record));
        } catch (PoisonEventException error) {
            metrics.consumed("unparsed", "dead_lettered");
            LOG.warnv("Dead-lettering malformed site event: topic={0} partition={1} offset={2} reason={3}", record.topic(),
                    record.partition(), record.offset(), error.getMessage());
            throw error;
        }
        if (parsed.isEmpty()) {
            // Another event type on the site topic. Acknowledged with no effect and no dedupe record.
            metrics.consumed("other", "ignored");
            return;
        }
        SiteEvent event = parsed.get();
        Span span = tracer.spanBuilder("inventory.consume " + event.eventType()).setParent(extract(event.traceparent()))
                .setAttribute("box1.site_id", event.siteId()).startSpan();
        MDC.put("siteId", event.siteId());
        MDC.put("eventId", event.eventId().toString());
        MDC.put("eventType", event.eventType());
        MDC.put("correlationId", event.correlationId());
        try (Scope ignored = span.makeCurrent()) {
            checkSiteHeader(record, event);
            try {
                String outcome = withRetries(event);
                metrics.consumed(event.eventType(), outcome);
                LOG.infov("Site event {0}", outcome);
            } catch (RuntimeException error) {
                span.recordException(error);
                metrics.consumed(event.eventType(), "dead_lettered");
                LOG.warnv("Dead-lettering site event: topic={0} partition={1} offset={2} error={3}", record.topic(),
                        record.partition(), record.offset(), error.getClass().getSimpleName());
                if (error instanceof PoisonEventException) {
                    throw error;
                }
                // Fixed text: this message and any cause reach dead-letter headers.
                throw new PoisonEventException("Retries exhausted");
            }
        } finally {
            span.end();
            MDC.remove("siteId");
            MDC.remove("eventId");
            MDC.remove("eventType");
            MDC.remove("correlationId");
        }
    }

    /** The header is informational until per-site Kafka identity exists (box1-docs#71, #131), so a mismatch is only logged. */
    private static void checkSiteHeader(ConsumerRecord<String, byte[]> record, SiteEvent event) {
        Header header = record.headers().lastHeader(SITE_HEADER);
        String value = header == null || header.value() == null ? null : new String(header.value(), StandardCharsets.UTF_8);
        if (!event.siteId().equals(value)) {
            LOG.warnv("Site event header {0} is missing or differs from the payload siteId", SITE_HEADER);
        }
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

    /**
     * Retries until the event applies. A database that cannot serve holds the channel without using up attempts, so a
     * valid site event is never dead-lettered for an outage. Everything else stays bounded by max-attempts.
     */
    private String withRetries(SiteEvent event) {
        long backoff = initialBackoffMillis;
        int attempt = 1;
        while (true) {
            try {
                return sites.apply(event);
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
                backoff = retryPause(backoff, attempt, error);
            } catch (RuntimeException error) {
                if (attempt >= maxAttempts) {
                    throw error;
                }
                attempt++;
                backoff = retryPause(backoff, attempt, error);
            }
        }
    }

    private long retryPause(long backoff, int attempt, RuntimeException error) {
        metrics.retried();
        LOG.warnv("Retrying site event: attempt={0} error={1}", attempt, error.getClass().getSimpleName());
        try {
            Thread.sleep(backoff);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted during event retry", interrupted);
        }
        return Math.min(backoff * 2, maxBackoffMillis);
    }

    private static Context extract(String traceparent) {
        if (traceparent == null) {
            return Context.current();
        }
        return W3CTraceContextPropagator.getInstance().extract(Context.current(), Map.of("traceparent", traceparent), GETTER);
    }
}
