# Runbook

## Health

| Check | Meaning |
|---|---|
| `/q/health/live` | The process is running. Stays up during broker and database outages. Goes down if the dead-letter topic cannot accept a poison record, because the channel stops. The pod then restarts and the record redelivers until the topic is available. |
| `/q/health/ready` | PostgreSQL is reachable. Kafka is deliberately excluded, so reads continue during a broker outage. A missing `box1.site.events.v1` topic does not affect it either. |

## Configuration

Set in `workloads/inventory-service/` in `box1-workloads-dev`.

| Variable | Default | Purpose |
|---|---|---|
| `QUARKUS_DATASOURCE_JDBC_URL`, `QUARKUS_DATASOURCE_USERNAME` | none | Database location and login |
| `QUARKUS_DATASOURCE_PASSWORD` | none | Secret reference only |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Broker address. Set this in every deployment. |
| `INVENTORY_ORDER_EVENTS_FLOOR` | **none, required** | Events with an older `occurredAt` have no effect. See below. |
| `INVENTORY_READ_POLICY` | `authenticated` | `permit` allows anonymous reads |
| `INVENTORY_RESERVATION_TTL` | `PT15M` | Life of an `ACTIVE` hold |
| `INVENTORY_EXPIRY_INTERVAL`, `INVENTORY_EXPIRY_BATCH` | `5s`, `100` | Expiry cadence and batch |
| `INVENTORY_EXPIRY_GRACE`, `INVENTORY_EXPIRY_BEHIND_THRESHOLD` | `PT2M`, `PT30S` | How long expiry waits after startup, a retry, an event older than the threshold, or an unreachable broker |
| `INVENTORY_METRICS_REFRESH_INTERVAL` | `15s` | Gauge refresh |
| `INVENTORY_OUTBOX_POLL_INTERVAL`, `INVENTORY_OUTBOX_BATCH_SIZE` | `1s`, `100` | Publisher poll and maximum claimed |
| `INVENTORY_OUTBOX_LEASE_SECONDS` | `60` | Claim lease |
| `INVENTORY_OUTBOX_BACKOFF_BASE_SECONDS`, `INVENTORY_OUTBOX_BACKOFF_MAX_SECONDS` | `1`, `300` | Publish retry backoff |
| `INVENTORY_CONSUMER_MAX_ATTEMPTS` | `20` | Attempts before dead-lettering. A database that cannot serve uses none. |
| `INVENTORY_CONSUMER_INITIAL_BACKOFF_MILLIS`, `INVENTORY_CONSUMER_MAX_BACKOFF_MILLIS` | `500`, `30000` | Retry delay, doubling to the cap |
| `INVENTORY_JWKS_URL`, `INVENTORY_JWT_ISSUER` | Keycloak realm `box1` | Token verification |

Site events use fixed properties in `application.properties`, not environment variables: `inventory.site.topic` (`box1.site.events.v1`), `inventory.site.group` (`inventory-site`) and `inventory.site.dlq.topic` (`box1.site.dlq.v1`). They share `INVENTORY_CONSUMER_MAX_ATTEMPTS` and the backoff settings above. `INVENTORY_ORDER_EVENTS_FLOOR` does not apply to them.

Put a change that must not orphan a ConfigMap in the Deployment's `env`, not the generated ConfigMap. Changing a `configMapGenerator` literal leaves the old hashed ConfigMap behind, Argo reports `OutOfSync`, and a release then fails at promote (box1-docs#42).

## First deployment

Merge in this order. Each merge needs its own approval.

1. **`box1-ops-contracts`.** Repin `contracts.ref` in `pom.xml` if the merge changed the commit.
2. **`box1-gitops` dead-letter topic.** Sync `box1-kafka`, then confirm `oc -n box1-kafka get kafkatopic box1.inventory.dlq.v1` is `Ready`.
3. **`box1-workloads-dev` floor.** The scaffold image ignores it. Re-read the topic first, per the next section.
4. **`inventory-service`.** Its release builds, runs the gate, promotes and smokes.

**If the service merges before the floor:** promote still merges the digest. The new pod refuses to start, the old pod keeps serving, and Argo stays unhealthy until promote times out and the run fails. Merge the floor, and Argo rolls forward.

## First-start offsets

Group `inventory` starts from the earliest retained offset. **An event of any type older than `INVENTORY_ORDER_EVENTS_FLOOR`, on either topic, is acknowledged with no hold, no outcome and no dedupe record**, then logged and counted as `box1_events_consumed_total{outcome="skipped_before_floor"}`. The comparison uses the envelope's `occurredAt`, so a replayed record stays excluded. A malformed record has no trustworthy `occurredAt`, so it is dead-lettered instead.

If the floor is missing, unparsable or in the future, the application refuses to start, logging `inventory.consumer.order-events-floor`, and the rollout keeps the previous pod.

Before a first deploy against a topic with retained events:

1. Read the topic and list what it retains. The output includes customer IDs, so do not paste it anywhere public:

   ```sh
   oc -n box1-kafka exec box1-kafka-box1-kafka-0 -- bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic box1.order.events.v1
   ```

2. Set the floor just after the newest record that must not be processed.
3. After rollout, confirm the offsets passed those records with the command below, that `box1.inventory.events.v1` has nothing for them, and that the orders did not change state.

Lowering the floor later does not rewind committed offsets. To process excluded events, reset the group's offsets too, and only with the Order owner's agreement, because each outcome moves a live order.

## Site topics and first start

`box1.site.events.v1` and `box1.site.dlq.v1` are `KafkaTopic`s in `box1-gitops` (`platform/kafka/topics/`), owned by the platform, not by this service. Create them before, or soon after, a release that contains the site consumer:

- **Topic missing:** the `site-events` consumer subscribes and waits; the Kafka client logs `UNKNOWN_TOPIC_OR_PARTITION` warnings. The pod stays live and Ready, the order consumers and reads are unaffected, and nothing is lost, because nothing can have been published to a topic that does not exist. The consumer sets `allow.auto.create.topics=false`, so it never makes the broker create the topic with default settings. Once the topic exists, the consumer is assigned its partitions on the next metadata refresh and starts from the earliest offset.
- **Dead-letter topic missing:** the broker has `auto.create.topics.enable` at its default (`true`), so the first poison site record creates `box1.site.dlq.v1` with broker defaults (1 partition, broker retention). Strimzi adopts it when the `KafkaTopic` arrives, but cannot reduce partitions. Create the dead-letter topic first.

Group `inventory-site` starts from the **earliest** retained offset and has no floor: every retained site event is applied once, which is the intent, because a site that was offline publishes its actions late with their original times. Before a first deploy against a site topic that already holds test events, read it (the command below with `--topic box1.site.events.v1`), and if any must not apply, ask before resetting the group's offsets past them.

```sh
oc -n box1-kafka exec box1-kafka-box1-kafka-0 -- bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic box1.site.events.v1
```

## Consumer lag

```sh
oc -n box1-kafka exec box1-kafka-box1-kafka-0 -- bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group inventory
oc -n box1-kafka exec box1-kafka-box1-kafka-0 -- bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group inventory-site
```

`box1_event_age_seconds` records only when a record arrives, so a stalled consumer shows nothing in it. Use this command, or the Kafka client lag meters on `/q/metrics`. No scrape is configured, so metrics here mean reading `/q/metrics`, and values reset on restart.

## Expiry waits for events

Order turns an expiry into `EXCEPTION`, so expiring while a fulfillment signal is still unread would damage a healthy order. For `INVENTORY_EXPIRY_GRACE`, expiry is held after startup, during and after any consumer retry, after an event arrives older than `INVENTORY_EXPIRY_BEHIND_THRESHOLD`, and whenever the broker has not answered within the grace. `box1_inventory_expiry_held_total` counts held polls. A held hold only keeps its units longer.

**Remaining limit:** an `inventory.reserved.v1` that sits unpublished in the outbox past its `expiresAt`, because the broker was down for longer than the TTL, still takes Order to `EXCEPTION`, by Order's own rule. Order must change for that to differ, so reconcile those orders after a long broker outage.

## Broker outage

**Signal:** the log shows `Unable to read dead-letter depth`, `box1_inventory_metrics_refresh_failures_total{source="kafka"}` rises and `box1_inventory_dlq_depth` reads `-1`. With work to publish, `box1_outbox_pending` also rises, and the log shows `Outbox publish not confirmed`.

Data stays safe. Committed outcomes stay in the outbox, consumption pauses with its offsets unmoved, and both catch up after recovery, within the 5-minute backoff cap. Reads keep working. Expiry waits. See the limit under [Expiry waits for events](#expiry-waits-for-events) for orders reserved just before a long outage.

## Pod crash

Consumption resumes when the broker expires the dead member's session, about 45 seconds by the Kafka default. Publishing resumes when the claim lease expires, 60 seconds by default. Uncommitted work rolls back and is redelivered. This delay is expected.

## Database outage

**Signal:** readiness returns 503, and the Route stops sending traffic. `box1_inventory_consumer_held_for_database_total` rises, and gauges keep their last values while `box1_inventory_metrics_refresh_failures_total{source="database"}` rises.

Consumers, including `inventory-site`, hold position for as long as the database cannot serve, retrying at most every 30 seconds. They use up no attempts, so valid events are not dead-lettered. Expiry waits until the grace after recovery. If the pod restarts while the database is down, Flyway fails at startup and the pod crash-loops until the database returns.

Other database errors are bounded. A value that can never be stored goes to the dead-letter topic at once. Anything else goes there after 20 attempts, about 7 minutes.

## Dead letters and replay

**Tested against a local broker only.** `box1.inventory.dlq.v1` must exist before this service deploys. If a dead-letter send fails, the channel stops and the pod restarts, and no record is lost. It keeps records for 14 days, against 7 for the order topic, and carries original values including `customerId`, so limit who reads it.

A dead-lettered event has its effects delayed. **A dead-lettered `fulfillment.requested.v1` means the hold can expire before you replay it**, so replay these first. A `shipment.confirmed.v1` leaves stock counted until replayed. An `order.placed.v1` leaves the order `SUBMITTED`.

1. Record who is replaying, why, and the time, with the incident notes. No automated audit record exists.
2. Read the topic. The `dead-letter-topic` and `dead-letter-reason` headers give the source and the cause. Replayed records stay here until retention removes them, so identify them by partition and offset, and keep that list with the notes:

   ```sh
   oc -n box1-kafka exec box1-kafka-box1-kafka-0 -- bin/kafka-console-consumer.sh \
     --bootstrap-server localhost:9092 --topic box1.inventory.dlq.v1 --from-beginning \
     --property print.key=true --property print.headers=true --property print.partition=true --property print.offset=true --timeout-ms 10000
   ```

3. Fix the cause. A malformed record cannot be replayed as it stands. Do not edit a business payload to make it pass.
4. Re-produce the record to its source topic with the same key and value, so `eventId` is unchanged. Replay is safe to repeat. **Producing to a shared topic needs the approval any shared-topic write needs.** Save the key, a tab and the value on one line in `record.txt`, then:

   ```sh
   oc -n box1-kafka exec -i box1-kafka-box1-kafka-0 -- bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
     --topic box1.order.events.v1 --property parse.key=true --property "key.separator=$(printf '\t')" < record.txt
   ```

5. Confirm it applied: `box1_events_consumed_total` for that type rises, and the reservation or order has the expected state. `box1_inventory_dlq_depth` does not fall.

### Site dead letters (`box1.site.dlq.v1`)

**Tested against a local broker only.** Records the `inventory-site` group could not process, unchanged, with the same `dead-letter-reason` and `dead-letter-topic` headers; `box1_inventory_site_dlq_depth` counts them. Reasons are `Record value is not valid JSON` or `... UTF-8`, a named envelope or payload field (`Invalid countedQuantity`, `aggregateId does not match siteId`, `Unsupported schemaVersion`, ...), `Value cannot be stored`, or `Retries exhausted`. A database outage never dead-letters a valid site event. Records carry no customer data, but do carry site, order and SKU identifiers.

A dead-lettered pick only delays its audit record. A dead-lettered count leaves `on_hand` at its previous value until replayed; replaying it later still gives the right result, because the rule uses `countedAt`, not arrival time, and a later count already applied simply supersedes it.

1. Record who is replaying, why, and the time, as above.
2. Read the topic, with partition and offset:

   ```sh
   oc -n box1-kafka exec box1-kafka-box1-kafka-0 -- bin/kafka-console-consumer.sh \
     --bootstrap-server localhost:9092 --topic box1.site.dlq.v1 --from-beginning \
     --property print.key=true --property print.headers=true --property print.partition=true --property print.offset=true --timeout-ms 10000
   ```

3. Fix the cause. A malformed record cannot be replayed as it stands: fix the producing site (`box1-org/site-gateway`), which re-sends from its outbox with the same `eventId`. Do not edit a business payload.
4. To replay a record that failed for a transient reason (`Retries exhausted`), re-produce it to `box1.site.events.v1` with the same key (the `siteId`) and value. **Producing to a shared topic needs approval.** Replay is safe to repeat: `eventId`, `pickId`, `countId` and the hold transition each stop a second effect.

   ```sh
   oc -n box1-kafka exec -i box1-kafka-box1-kafka-0 -- bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
     --topic box1.site.events.v1 --property parse.key=true --property "key.separator=$(printf '\t')" < record.txt
   ```

5. Confirm it applied: `GET /api/v1/site-events?siteId=<siteId>` shows the decision, and `box1_events_consumed_total{type="site..."}` rises.

## Stock and reservation operations

Nothing here works without a token from a Keycloak client that has the `inventory-admin` role. **No such client exists yet (box1-docs#12, D-010).** Until it does, these routes reject every call.

```sh
curl -X POST -H "Authorization: Bearer $TOKEN" -H 'Idempotency-Key: <unique>' -H 'Content-Type: application/json' \
  -d '{"quantity":5,"reason":"Cycle count"}' \
  https://inventory-service-dev.apps.ocp.box1.org/internal/v1/stock/site_nyc/SKU-42/restock

curl -X POST -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"reason":"Order reconciled by hand"}' \
  https://inventory-service-dev.apps.ocp.box1.org/internal/v1/reservations/<id>/release
```

Both are recorded with the token's subject. Release returns 409 for a consumed or expired reservation.

**Release tells Order nothing, because the contracts have no such event.** Order stays in `FULFILLMENT_REQUESTED`, and a request already sent can still ship, which then opens `SHIPMENT_AFTER_RELEASE`. So reconcile the order with its owner before releasing a `COMMITTED` hold.
 The lab fixture comes from Flyway migration `V2`.

### Creating sites and stock records

Two create-once routes add sites and stock records, for example the scenario controller's demo fixtures (box1-docs#19). They never change or delete an existing site or record, so the `V2` lab fixture is safe from them.

```sh
curl -X PUT -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"status":"ACTIVE","reason":"Fixture seed"}' \
  https://inventory-service-dev.apps.ocp.box1.org/internal/v1/sites/site_spacedock

curl -X PUT -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"openingQuantity":500,"reason":"Fixture seed"}' \
  https://inventory-service-dev.apps.ocp.box1.org/internal/v1/stock/site_spacedock/CAT-SPOT-BOWL
```

| Route | 201 | 200 | 409 | 404 |
|---|---|---|---|---|
| `PUT /internal/v1/sites/{siteId}` | Created | Exists with this status; nothing changed | Exists with another status | — |
| `PUT /internal/v1/stock/{siteId}/{sku}` | Created with `openingQuantity` on hand | Opened earlier with the same quantity; returns the current level, nothing changed | Seeded record, or opened with another quantity | No such site |

A site creation is recorded in the immutable `site_registration` table, and a stock record's opening quantity as an `OPENING` stock movement, both with the token's subject and reason. There is no route to change a site's status or remove anything: do that, if ever, with a reviewed forward migration.

## Reconciliation

`box1_inventory_reconciliation_open` counts rows in `reconciliation_item`. Inventory opened each because it could not resolve a fact alone. No tooling marks them `RESOLVED` in v1; the operations console does not show them yet.

Read them, newest first, with any token the read policy accepts:

```sh
curl -H "Authorization: Bearer $TOKEN" 'https://inventory-service-dev.apps.ocp.box1.org/api/v1/reconciliation-items?status=OPEN&limit=50'
curl -H "Authorization: Bearer $TOKEN" 'https://inventory-service-dev.apps.ocp.box1.org/api/v1/reconciliation-items?kind=COUNT_CONFLICT&siteId=site_ds9'
curl -H "Authorization: Bearer $TOKEN" 'https://inventory-service-dev.apps.ocp.box1.org/api/v1/site-events?siteId=site_ds9&type=COUNT'
```

Filters: `status`, `kind`, `siteId`, `orderId`, `limit` (1–100), `offset`. Order-flow items carry `orderId`; site count and hold items carry `siteId`, `sku` and `detail`. `/api/v1/site-events` lists every recorded pick, count and hold with its outcome, and for an applied count `expected`, `laterMovements`, `reserved`, `onHandBefore` and `onHandAfter`.

| Kind | Meaning | Usual response |
|---|---|---|
| `SHIPMENT_AFTER_RELEASE` | Goods shipped after the hold expired or was released. `stock_adjusted` says whether `on_hand` could be reduced. It is reduced at most once per hold. | Correct `on_hand` if it could not, and review the order |
| `SHIPMENT_WITHOUT_RESERVATION` | A shipment for an order with no hold here | Check the order and the producer |
| `SHIPMENT_DUPLICATE` | A second, different `shipmentId` for a hold whose stock already moved | Check with the ERP |
| `SHIPMENT_REQUEST_MISMATCH` | A shipment naming a different fulfillment request from the committed one. Nothing moved. | Check the producer and the order |
| `SHIPMENT_FAILED` | Permanent shipment failure. A `COMMITTED` hold stays. An `ACTIVE` hold can still expire on its TTL. | Decide, then release if the goods did not move and the hold is still held |
| `FULFILLMENT_AFTER_RELEASE` | Fulfillment requested after the hold ended. After an expiry, Order also goes to `EXCEPTION`. After a release, Order hears nothing. | Reconcile both |
| `FULFILLMENT_WITHOUT_RESERVATION` | The signal named a reservation this order does not own | Check the producer |
| `CANCELLED_AFTER_COMMIT` | An order was cancelled after its hold committed | Decide, then release |
| `PICK_NOT_HELD` | A site picked units for a reservation that had expired or been released. Stock and the reservation are unchanged. `detail.reservationStatus` says which. | Find the units: put them back (a new count records them), or reconcile the order with its owner if they shipped. A released hold stays on the site's pick list until it expires or ships, because release publishes no event |
| `PICK_MISMATCH` | A pick for an unknown reservation, or whose `orderId`, `siteId`, `sku` or `quantity` differs. `detail.differs` lists the fields; `detail.reservation` is `UNKNOWN` when none exists. Nothing changed. | Check the site's pick list and the producing site; correct the physical stock with a count if units moved |
| `COUNT_CONFLICT` | A count implied fewer units (`detail.expected`) than are reserved, or fewer than zero. `on_hand` was held at `max(expected, reserved, 0)` (`detail.onHandAfter`), so `detail.shortfall` units are held but may not exist. No reservation was cancelled. | Recount at the site. If the units are really missing, decide which held orders cannot be filled and release them; the next count then applies normally |
| `COUNT_UNKNOWN_STOCK` | A count for a SKU the site has no stock record for. Nothing changed. | If the SKU belongs at the site, create the record (`PUT /internal/v1/stock/{siteId}/{sku}`), then have the site count again |
| `HOLD_UNKNOWN` | A hold `RELEASED` that was never `PLACED` here. Holds change no stock in v1. | Check the site's hold records; usually a lost or dead-lettered `PLACED` event |

The site kinds never change a reservation. A later count supersedes an earlier one by `countedAt`, so a recount is the normal way to clear a count discrepancy.

A long `box1_inventory_committed_oldest_age_seconds` means a committed hold is waiting on fulfillment. That is correct while no adapter runs, and the units stay held.

## Release and rollback

`.tekton/release.yaml` runs build, `box1-security-gates` with `gate`, promote, shared smoke, and `inventory-api-smoke`.

Reverting the promotion in `box1-workloads-dev` restores the previous image. **Not exercised.** Flyway does not roll back: `V1` to `V4` stay applied, and any schema repair is a forward migration. `V4` is additive, so an image before it runs against it: it never reads the site tables or `business_at`, writes shipment movements with `business_at` NULL (read as their processing time), and always writes an `order_id` on reconciliation items. Its group `inventory-site` simply stops; on return, consumption resumes from the committed offset. The onboarding scaffold ignores those tables, so reverting to it is safe for the data, but events keep waiting, and holds stop expiring, until the service returns.

Database backup and restore follow `postgres/STORAGE-BACKUP-RESTORE.md` in `box1-ops-contracts`.
