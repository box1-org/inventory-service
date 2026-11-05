package org.box1.inventory.domain;

/** Reservation lifecycle from box1-ops-contracts INVENTORY-RESERVATIONS.md. */
public enum ReservationStatus {
    ACTIVE, COMMITTED, CONSUMED, EXPIRED, RELEASED
}
