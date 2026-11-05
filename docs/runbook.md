# Runbook

## Health

| Check | Meaning |
|---|---|
| `/q/health/live` | The process is running. Stays up during broker and database outages. Goes down if the dead-letter topic cannot accept a poison record, because the channel stops. The pod then restarts and the record redelivers until the topic is available. |
| `/q/health/ready` | PostgreSQL is reachable. Kafka is deliberately excluded, so reads continue during a broker outage. |

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
| `INVENTORY_OUTBOX_POLL_INTERVAL`, `INVENTORY_OUTBOX_BATCH_SIZE` | `1s`, `100` | Publisher poll and maximum claimed |
| `INVENTORY_OUTBOX_LEASE_SECONDS` | `60` | Claim lease |
| `INVENTORY_OUTBOX_BACKOFF_BASE_SECONDS`, `INVENTORY_OUTBOX_BACKOFF_MAX_SECONDS` | `1`, `300` | Publish retry backoff |
| `INVENTORY_CONSUMER_MAX_ATTEMPTS` | `20` | Attempts before dead-lettering |
| `INVENTORY_CONSUMER_INITIAL_BACKOFF_MILLIS`, `INVENTORY_CONSUMER_MAX_BACKOFF_MILLIS` | `500`, `30000` | Retry delay, doubling to the cap |
| `INVENTORY_JWKS_URL`, `INVENTORY_JWT_ISSUER` | Keycloak realm `box1` | Token verification |

Put a change that must not orphan a ConfigMap in the Deployment's `env`, not the generated ConfigMap. Changing a `configMapGenerator` literal leaves the old hashed ConfigMap behind, Argo reports `OutOfSync`, and a release then fails at promote (box1-docs#42).

## First-start offsets

Group `inventory` starts from the earliest retained offset. **An `order.placed.v1` older than `INVENTORY_ORDER_EVENTS_FLOOR` is acknowledged with no hold, no outcome and no dedupe record**, then logged and counted as `box1_events_consumed_total{outcome="skipped_before_floor"}`. The comparison uses the envelope's `occurredAt`, so a replayed record stays excluded.

Without a valid floor the application refuses to start, logging `inventory.consumer.order-events-floor`, and the rollout keeps the previous pod.

Before a first deploy against a topic with retained events:

1. Read the topic and list what it retains.
2. Set the floor just after the newest record that must not be processed.
3. After rollout, confirm the group's offsets passed those records, `box1.inventory.events.v1` is empty for them, and the orders did not change state.

Lowering the floor later does not rewind committed offsets. To process excluded events, reset the group's offsets too, and only with the Order owner's agreement, because each outcome moves a live order.

## Broker outage

**Signal:** `box1_outbox_pending` rises, and the log shows `Outbox publish not confirmed`.

No action is needed for data safety. Committed outcomes stay in the outbox, consumption pauses with its offsets unmoved, and both catch up after recovery, within the 5-minute backoff cap. Reads keep working.

## Pod crash

Consumption resumes when the broker expires the dead member's session, about 45 seconds by the Kafka default. Publishing resumes when the claim lease expires, 60 seconds by default. Uncommitted work rolls back and is redelivered. This delay is expected.

## Database outage

**Signal:** readiness returns 503, and the Route stops sending traffic. `box1_event_retries_total` rises, and gauges keep their last values while `box1_inventory_metrics_refresh_failures_total` rises.

Consumers hold position and retry for about 7 minutes. Beyond that, events go to the dead-letter topic, so replay them as below after recovery. If the pod restarts while the database is down, Flyway fails at startup and the pod crash-loops until the database returns.

## Dead letters and replay

**Tested against a local broker only.** Local tests cover re-producing a dead-lettered record to its source topic and confirm it applies once. `box1.inventory.dlq.v1` must exist before this service deploys. Otherwise the broker may auto-create it with default retention.

1. Record who is replaying, why, and the time, with the incident notes. No automated audit record exists.
2. Read the topic. The `dead-letter-topic` and `dead-letter-reason` headers give the source and the cause:

   ```sh
   oc -n box1-kafka exec box1-kafka-box1-kafka-0 -- bin/kafka-console-consumer.sh \
     --bootstrap-server localhost:9092 --topic box1.inventory.dlq.v1 --from-beginning \
     --property print.key=true --property print.headers=true --timeout-ms 10000
   ```

3. Fix the cause. A malformed record cannot be replayed as it stands. Do not edit a business payload to make it pass.
4. Re-produce the record to its source topic with the same key and the same value, so `eventId` is unchanged. Replay is safe to repeat, and the same `eventId` applies once. **Producing to a shared topic needs the approval that any shared-topic write needs.**
5. Confirm `box1_inventory_dlq_depth` is unchanged or expected. Records stay on the topic until retention removes them.

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

Both are recorded with the token's subject. Release returns 409 for a consumed or expired reservation. Fixture data comes only from Flyway migration `V2`. Until the client exists, the only way to change stock is a forward migration through a pull request.

## Reconciliation

`box1_inventory_reconciliation_open` counts rows in `reconciliation_item`. Inventory opened each because it could not resolve a fact alone. No tooling resolves them in v1.

| Kind | Meaning | Usual response |
|---|---|---|
| `SHIPMENT_AFTER_RELEASE` | Goods shipped after the hold expired or was released. `stock_adjusted` says whether `on_hand` could be reduced. | Correct `on_hand` if it could not, and review the order |
| `SHIPMENT_WITHOUT_RESERVATION` | A shipment for an order with no hold here | Check the order and the producer |
| `SHIPMENT_DUPLICATE` | A second, different `shipmentId` for a consumed hold | Check with the ERP |
| `SHIPMENT_FAILED` | Permanent shipment failure. The hold stays. | Decide, then release if the goods did not move |
| `FULFILLMENT_AFTER_RELEASE` | Fulfillment requested after the hold ended | Order also goes to `EXCEPTION`, so reconcile both |
| `FULFILLMENT_WITHOUT_RESERVATION` | The signal named a reservation this order does not own | Check the producer |
| `CANCELLED_AFTER_COMMIT` | An order was cancelled after its hold committed | Decide, then release |

A long `box1_inventory_committed_oldest_age_seconds` means a committed hold is waiting on fulfillment. That is correct while no adapter runs, and the units stay held.

## Release and rollback

`.tekton/release.yaml` runs build, `box1-security-gates` with `gate`, promote, shared smoke, and `inventory-api-smoke`.

Reverting the promotion in `box1-workloads-dev` restores the previous image. **Not exercised.** Flyway does not roll back: `V1` and `V2` stay applied, and any schema repair is a forward migration. The onboarding scaffold ignores those tables, so reverting to it is safe for the data, but events keep waiting, and holds stop expiring, until the service returns.

Database backup and restore follow `postgres/STORAGE-BACKUP-RESTORE.md` in `box1-ops-contracts`.
