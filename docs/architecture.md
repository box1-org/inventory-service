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
| `stock_movement` | Immutable: every `on_hand` change, the actor and the reason. `occurred_at` is when it was recorded; `business_at` (V4) is when it happened, where that differs |
| `reconciliation_item` | Facts Inventory cannot resolve alone, for a person. Site kinds (V4) carry `site_id`, `sku` and a `detail` object |
| `processed_event` | Inbound `eventId`s, for dedupe, from every consumer |
| `site_pick`, `site_count`, `site_hold` | Immutable (V4): every site pick, cycle count and hold transition, and the decision taken. Unique `pick_id`, `count_id` and `(hold_id, action)` |
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

## Site events

Edge sites publish `site.pick-confirmed.v1`, `site.stock-adjusted.v1` and `site.stock-held.v1` on `box1.site.events.v1`, keyed by `siteId` ([SITE.md](https://github.com/box1-org/box1-ops-contracts/blob/5b7296ca87aeed7a4f89a443e45931def4706658/SITE.md) in the contracts, "Effect in inventory-service", is normative). The service consumes them as group `inventory-site`. Site events never create, release or consume a reservation, never change `reserved`, never publish an event, and never change the order → ERP → ship flow. Gating an order on its pick is the future enhancement box1-docs#134.

Each event is one transaction: the `processed_event` row, the site record, any stock change and any reconciliation item commit together. A repeated `eventId` stops at `processed_event`. A repeated `pickId`, `countId` or hold transition `(holdId, action)` under a new `eventId` is also a duplicate and changes nothing. Other event types on the topic are acknowledged and ignored. No `INVENTORY_ORDER_EVENTS_FLOOR` applies to this topic.

### Picks

A pick records that the reserved units physically left their location. It does **not** change `on_hand` or `reserved`, because `shipment.confirmed.v1` already reduces both, and the same units must not leave stock twice. The pick is compared with the reservation it names:

| Reservation `reservationId` | `site_pick.outcome` | Reconciliation item |
|---|---|---|
| `ACTIVE` or `COMMITTED`, same `orderId`, `siteId`, `sku` and `quantity` | `PICKED` | none |
| `CONSUMED`, same fields (the shipment was confirmed first) | `PICKED_AFTER_SHIPMENT` | none |
| `EXPIRED` or `RELEASED`, same fields | `NOT_HELD` | `PICK_NOT_HELD` |
| Unknown, or any of `orderId`, `siteId`, `sku`, `quantity` differs | `MISMATCH` | `PICK_MISMATCH`, with `detail.differs` |

Identity is checked before status: a pick whose fields differ from an expired reservation is a `PICK_MISMATCH`, because it does not describe that reservation. The reservation status the decision saw is kept in `site_pick.reservation_status`.

### Cycle counts: the rule

A count `C` of `sku` at `siteId`, taken at `countedAt` `T`, states how many units were physically at the site at `T`, including units picked but not yet shipped. With the stock row locked (`SELECT ... FOR UPDATE`, so no reservation, shipment or restock interleaves):

1. No stock row for the site and SKU: nothing changes, the count is recorded `UNKNOWN_STOCK`, and `COUNT_UNKNOWN_STOCK` is opened.
2. A count of the same site and SKU with a later `countedAt` (on a tie, the greater `countId`, compared in byte order) was already applied: nothing changes and the count is recorded `SUPERSEDED`.
3. Otherwise `E = C + D`, where `D` is the sum of the site and SKU's `stock_movement` deltas, other than `COUNT`, whose business time is strictly after `T`.
4. `on_hand := max(E, reserved, 0)`, recorded as a `COUNT` movement with the delta (which may be zero), `reference` = `countId`, idempotency key `site-count:<countId>`, and `occurred_at` = `business_at` = `countedAt`. The count is recorded `APPLIED` with `E`, `D`, `reserved` and `on_hand` before and after.
5. If `E < reserved` or `E < 0`, `on_hand` is held at that limit and `COUNT_CONFLICT` is opened with `expected` (E), `reserved` and `shortfall` (the units kept above E). Reservations are never cancelled by a count.

The latest count wins, so the result does not depend on the order in which counts arrive, as long as the movements and `reserved` are the same.

**Business time.** A movement's business time is `COALESCE(business_at, occurred_at)`:

| Kind | Business time |
|---|---|
| `SHIPMENT`, `LATE_SHIPMENT` | `shippedAt` from `shipment.confirmed.v1`, stored in `business_at` from V4 on |
| `RESTOCK`, `OPENING`, `SEED` | `occurred_at`, the time the service recorded it, which is when the stock arrived |
| `COUNT` | `countedAt` (excluded from D) |

Before V4, `occurred_at` of a shipment was the time the consumer processed the event, not `shippedAt`, and `shippedAt` was validated and dropped. V4 adds `business_at` and the shipment path now stores `shippedAt` in it; nothing else about shipment handling changed. Shipment movements recorded before V4 have no `business_at`, so the rule uses their processing time.

**Effect of `shipment.confirmed.v1`** (unchanged): for an `ACTIVE` or `COMMITTED` hold (with the committed request), the reservation becomes `CONSUMED`, `on_hand` and `reserved` both fall by the quantity in one statement, so available is unchanged, and a `SHIPMENT` movement of minus the quantity is written. For an `EXPIRED` or `RELEASED` hold, `on_hand` alone falls once if available covers it (`LATE_SHIPMENT`) and `SHIPMENT_AFTER_RELEASE` is opened. A pick never repeats either change.

**Known limits.** Clocks differ: `countedAt` is the site's clock, `shippedAt` the ERP's, restocks the service's. A movement that happened before `T` but is recorded only after the count was applied (for example a shipment confirmed late by the ERP) is applied on top of the count, so it is taken twice; the count rule cannot see it in advance, and a later count corrects it. A count that arrives before the shipment that left before it was taken has the same effect.

### Holds

A hold is recorded by `holdId`, one row per transition (`PLACED`, `RELEASED`) with quantity, reason and time. In v1 it changes neither `on_hand` nor available stock; the site uses it to keep units off its own pick work. A `RELEASED` for a hold never placed is recorded `UNKNOWN_HOLD` and opens `HOLD_UNKNOWN`. Making holds reduce available stock is a future change.

### Errors

A record that is not UTF-8 JSON, or whose envelope or payload does not validate, goes to `box1.site.dlq.v1` unchanged, with `dead-letter-reason` and `dead-letter-topic` headers. A database that cannot serve holds the record without using attempts, as for the order consumers; other failures dead-letter after `INVENTORY_CONSUMER_MAX_ATTEMPTS` with the reason `Retries exhausted`. Site consumer retries do not hold reservation expiry. The `box1-site-id` header is informational until per-site Kafka identity exists (box1-docs#71, #131): a missing or different header is logged and otherwise ignored.

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
| `/api/v1/inventory`, `/api/v1/reservations/{id}`, `/api/v1/reconciliation-items`, `/api/v1/site-events` | `INVENTORY_READ_POLICY`. Anything but an exact `permit` requires a valid token |
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
| `box1_inventory_site_dlq_depth` | Records on `box1.site.dlq.v1`. `-1` means unknown, including while the topic does not exist |
| `box1_inventory_expiry_held_total` | Expiry polls held back because events may be behind |
| `box1_inventory_consumer_held_for_database_total` | Retries while the database could not serve |
| `box1_inventory_metrics_refresh_failures_total{source}` | Failed gauge refreshes |
| `box1_outbox_published_total`, `box1_outbox_publish_failures_total`, `box1_outbox_publish_lag_seconds_*` | Publisher results |
| `box1_inventory_reservations_active`, `box1_inventory_reservations_committed` | Open holds |
| `box1_events_consumed_total{type,outcome}` | Consumed events, including `skipped_before_floor` and `dead_lettered`. Site events use their own types and outcomes such as `count_applied`, `count_conflict`, `pick_mismatch`, `duplicate_pick`; other types on the site topic count as `type="other",outcome="ignored"` |
| `box1_outbox_pending`, `box1_outbox_oldest_pending_age_seconds` | Publishing backlog |
| `box1_inventory_committed_oldest_age_seconds` | How long the oldest committed hold has waited |
| `box1_inventory_reconciliation_open` | Items waiting for a person |
| `box1_inventory_reserved_drift` | Stock rows whose `reserved` disagrees with their holds. Must be 0 |

Counters appear only after their first increment. Dead-letter lines and the expiry causation are worth knowing when reading traces: `inventory.reservation-expired.v1` names the placing `order.placed.v1` as its `causationId`.
