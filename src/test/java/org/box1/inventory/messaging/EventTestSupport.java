package org.box1.inventory.messaging;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.box1.inventory.domain.ReasonCode;
import org.box1.inventory.domain.Reservation;
import org.box1.inventory.domain.ReservationStatus;
import org.box1.inventory.store.OutboxStore;

/** Captures what EventFactory would enqueue, without a database. */
public final class EventTestSupport {
    private EventTestSupport() {}

    public static Map<String, JsonNode> samples(ObjectMapper mapper, Instant now) {
        Map<String, JsonNode> produced = new LinkedHashMap<>();
        EventFactory factory = new EventFactory(mapper, new OutboxStore(null) {
            @Override
            public void enqueue(UUID eventId, String aggregateId, String eventType, String envelopeJson) {
                try {
                    produced.put(eventType, mapper.readTree(envelopeJson));
                } catch (com.fasterxml.jackson.core.JsonProcessingException error) {
                    throw new IllegalStateException(error);
                }
            }
        });
        UUID cause = UUID.randomUUID();
        Reservation hold = new Reservation("res_0123456789abcdef", "ord_1042", "site_nyc", "SKU-42", 2, ReservationStatus.ACTIVE,
                now.plusSeconds(900), null, null, cause, "corr-1042", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", now, now);
        factory.reserved(hold, cause, now);
        factory.rejected("ord_1043", "SKU-42", 2, "site_nyc", ReasonCode.INSUFFICIENT_STOCK, "corr-1043", cause, hold.traceparent(), now);
        factory.expired(hold, now);
        return produced;
    }
}
