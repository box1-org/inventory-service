package org.box1.inventory.domain;

import java.time.Instant;

/** Stock at one site. Available is on hand minus active holds. */
public record StockLevel(String siteId, String sku, int onHand, int reserved, Instant updatedAt) {
    public int available() {
        return onHand - reserved;
    }
}
