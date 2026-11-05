package org.box1.inventory.api;

import java.time.Instant;

import org.box1.inventory.domain.Reservation;
import org.box1.inventory.domain.StockLevel;

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
}
