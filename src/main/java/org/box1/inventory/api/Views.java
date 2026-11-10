package org.box1.inventory.api;

import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;
import org.box1.inventory.domain.Reservation;
import org.box1.inventory.domain.StockLevel;
import org.box1.inventory.store.SiteStore;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/** API response bodies. Internal correlation and audit fields stay out of them. */
public final class Views {
    private Views() {}

    public record Stock(String siteId, String sku, int onHand, int reserved, int available, Instant updatedAt) {
        static Stock of(StockLevel level) {
            return new Stock(level.siteId(), level.sku(), level.onHand(), level.reserved(), level.available(), level.updatedAt());
        }
    }

    public record Hold(String reservationId, String orderId, String siteId, String sku, int quantity, String status,
            Instant expiresAt, Instant createdAt, Instant updatedAt) {
        static Hold of(Reservation value) {
            return new Hold(value.reservationId(), value.orderId(), value.siteId(), value.sku(), value.quantity(),
                    value.status().name(), value.expiresAt(), value.createdAt(), value.updatedAt());
        }
    }

    public record Restock(Integer quantity, String reason) {}

    public record Release(String reason) {}

    public record SiteRequest(String status, String reason) {}

    public record Site(String siteId, String status) {}

    public record StockRequest(Integer openingQuantity, String reason) {}

    /** A fact Inventory could not resolve alone. Order-flow kinds carry orderId; count and hold kinds carry siteId and sku. */
    public record ReconciliationItem(long seq, String kind, String status, String orderId, String reservationId, String siteId,
            String sku, String shipmentId, boolean stockAdjusted, UUID eventId,
            @Schema(type = SchemaType.OBJECT, description = "Kind-specific facts, for example expected, reserved and shortfall of a COUNT_CONFLICT")
            JsonNode detail, Instant createdAt) {
        static ReconciliationItem of(SiteStore.ReconciliationRow row, JsonNode detail) {
            return new ReconciliationItem(row.seq(), row.kind(), row.status(), row.orderId(), row.reservationId(), row.siteId(),
                    row.sku(), row.shipmentId(), row.stockAdjusted(), row.eventId(), detail, row.createdAt());
        }
    }

    /**
     * A recorded site decision. type PICK has orderId, reservationId and reservationStatus; COUNT has the rule's values
     * when applied; HOLD has action. businessAt is pickedAt, countedAt or changedAt.
     */
    public record SiteDecision(@Schema(enumeration = {"PICK", "COUNT", "HOLD"}) String type, String id, String action,
            UUID eventId, String siteId, String sku, int quantity, String orderId, String reservationId, Instant businessAt,
            String outcome, String reservationStatus, Integer expected, Integer laterMovements, Integer reserved,
            Integer onHandBefore, Integer onHandAfter, String reason, Instant recordedAt) {
        static SiteDecision of(SiteStore.SiteDecisionRow row) {
            return new SiteDecision(row.type(), row.id(), row.action(), row.eventId(), row.siteId(), row.sku(), row.quantity(),
                    row.orderId(), row.reservationId(), row.businessAt(), row.outcome(), row.reservationStatus(), row.expected(),
                    row.laterMovements(), row.reserved(), row.onHandBefore(), row.onHandAfter(), row.reason(), row.recordedAt());
        }
    }
}
