# Architecture

## Boundary

Inventory owns its stock and reservation data, in its own PostgreSQL instance. It never reads or writes another service's tables, and never updates an order. Order learns of outcomes only through `box1.inventory.events.v1`.

## Data model

| Table | Holds |
|---|---|
| `inventory_site` | Known sites and whether they can take holds |
| `inventory_stock` | `on_hand` and `reserved` per site and SKU. Available is `on_hand - reserved`, and a `CHECK` keeps it from going negative |
| `reservations` | One hold per order, with its status and expiry |
| `order_outcome` | The single decision for each order |
| `reservation_history` | Immutable: every status change, the actor and the event |
| `stock_movement` | Immutable: every `on_hand` change, the actor and the reason |
| `reconciliation_item` | Facts Inventory cannot resolve alone, for a person |
| `processed_event` | Inbound `eventId`s, for dedupe |
| `outbox` | Outbound events, written in the business transaction |

## Reservation lifecycle

```text
                      fulfillment.requested.v1            shipment.confirmed.v1
order.placed.v1 -> ACTIVE ---------------------> COMMITTED ---------------------> CONSUMED
                     | | \\                          |
          TTL elapsed| | \\ shipment.confirmed.v1     | authenticated operator release
                     | |  \\------------> CONSUMED    v
                     v |                           RELEASED
                 EXPIRED| cancelled or operator release
                        v
                     RELEASED
```

- **Why the signal exists.** Order moves `FULFILLMENT_REQUESTED` to `EXCEPTION` on `inventory.reservation-expired.v1`. So a hold stops being able to expire once Inventory has read Order's fulfillment request. Without that, a slow ERP would turn a healthy order into an exception. Expiry also waits while events may be unread: after startup, during and after a consumer retry, after a late event, and while the broker is unreachable.
- **Why `ACTIVE` can go straight to `CONSUMED`.** Two topics have no mutual ordering, so a shipment can overtake the signal.
- **Terminal states** are `CONSUMED`, `EXPIRED` and `RELEASED`. A confirmed shipment never releases stock. A shipment for an expired or released hold opens a reconciliation item, and reduces `on_hand` only if available covers it.

## Consistency

Reservation is one statement: `UPDATE inventory_stock SET reserved = reserved + q WHERE ... AND on_hand - reserved >= q`, at READ COMMITTED. PostgreSQL locks the row and re-checks the predicate against the committed row, so two orders for the last unit serialize and the loser matches nothing. There is no version column, because there is no read-modify-write.

The stock change, the reservation, its history, `processed_event`, `order_outcome` and the outbox row are one transaction. There is no distributed transaction and no claim of exactly-once delivery. Delivery is at least once, and duplicates are harmless: a repeated `eventId` stops at `processed_event`, and the same order under a new `eventId` stops at `order_outcome`, even if stock has changed since.

## Publishing

The publisher claims a bounded batch with `FOR UPDATE SKIP LOCKED` under a lease, sends with `acks=all` outside any transaction, and marks rows published only after the broker confirms. Only the oldest unpublished event per order is claimable, which keeps per-order order. A crash after the broker confirms re-sends the same `eventId`, and consumers dedupe on it.

## Clocks

An injected `Clock` decides `expires_at`, what is due and `expiredAt`. Outbox claim and retry times come from the database clock. The two are never compared.

## Access

| Path | Access |
|---|---|
| `/api/greetings`, `/api/info`, `/q/health/*`, `/q/openapi`, `/q/metrics`, `/q/swagger-ui` | Open, because the release smoke task requires them |
| `/api/v1/inventory`, `/api/v1/reservations/{id}` | `INVENTORY_READ_POLICY`. Anything but an exact `permit` requires a valid token |
| `/internal/v1/**` | Always needs a verified JWT with role `inventory-admin` under `resource_access.inventory-service.roles` |

The service exposes every path through its Route, so the check is enforced in the service itself, against that client's own claim. A top-level `groups` claim does not count. It holds no client secret. It verifies signature, expiry, issuer and audience against the realm's public keys, and a key fetch failure rejects the request.

`customerId` arrives in `order.placed.v1`, and is checked for presence and otherwise dropped. It is not stored, logged or republished. Dead-letter records hold original values unchanged, so that topic is as sensitive as its source.

## Observability

- **Logs** are JSON in the deployed image, with `orderId`, `eventId`, `eventType`, `correlationId` and trace IDs in the MDC.
- **Traces** continue the inbound W3C `traceparent` through the outbox into published events. Export is off until a collector exists (box1-docs#22).
- **Metrics** are on `/q/metrics`:

| Metric | Meaning |
|---|---|
| `box1_inventory_reserved_units_total{site}` | Units reserved by site |
| `box1_inventory_rejections_total{reason}` | Denials by reason |
| `box1_inventory_conflict_retries_total` | Retries caused by a conflict |
| `box1_event_retries_total` | All event retries |
| `box1_event_age_seconds_count`, `_sum`, `_max` | Age of an event on arrival. Silent when the consumer is stalled, so see the runbook for lag |
| `box1_inventory_reservations_expired_total` | Expired holds |
| `box1_inventory_dlq_depth` | Records on the dead-letter topic. `-1` means unknown |
| `box1_inventory_expiry_held_total` | Expiry polls held back because events may be behind |
| `box1_inventory_consumer_held_for_database_total` | Retries while the database could not serve |
| `box1_inventory_metrics_refresh_failures_total{source}` | Failed gauge refreshes |
| `box1_outbox_published_total`, `box1_outbox_publish_failures_total`, `box1_outbox_publish_lag_seconds_*` | Publisher results |
| `box1_inventory_reservations_active`, `box1_inventory_reservations_committed` | Open holds |
| `box1_events_consumed_total{type,outcome}` | Consumed events, including `skipped_before_floor` and `dead_lettered` |
| `box1_outbox_pending`, `box1_outbox_oldest_pending_age_seconds` | Publishing backlog |
| `box1_inventory_committed_oldest_age_seconds` | How long the oldest committed hold has waited |
| `box1_inventory_reconciliation_open` | Items waiting for a person |
| `box1_inventory_reserved_drift` | Stock rows whose `reserved` disagrees with their holds. Must be 0 |

Counters appear only after their first increment. Dead-letter lines and the expiry causation are worth knowing when reading traces: `inventory.reservation-expired.v1` names the placing `order.placed.v1` as its `causationId`.
