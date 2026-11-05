-- Available is on_hand - reserved. The CHECK is what keeps it from going negative, whatever the code does.
CREATE TABLE inventory_site (
    site_id text PRIMARY KEY,
    status text NOT NULL CHECK (status IN ('ACTIVE','UNAVAILABLE'))
);

CREATE TABLE inventory_stock (
    site_id text NOT NULL REFERENCES inventory_site (site_id),
    sku text NOT NULL,
    on_hand integer NOT NULL CHECK (on_hand >= 0),
    reserved integer NOT NULL DEFAULT 0 CHECK (reserved >= 0),
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (site_id, sku),
    CONSTRAINT available_not_negative CHECK (on_hand >= reserved)
);

CREATE TABLE reservations (
    reservation_id text PRIMARY KEY,
    order_id text NOT NULL UNIQUE,
    site_id text NOT NULL,
    sku text NOT NULL,
    quantity integer NOT NULL CHECK (quantity > 0),
    status text NOT NULL CHECK (status IN ('ACTIVE','COMMITTED','CONSUMED','EXPIRED','RELEASED')),
    expires_at timestamptz NOT NULL,
    fulfillment_request_id text NULL,
    shipment_id text NULL,
    placed_event_id uuid NOT NULL,
    correlation_id text NOT NULL,
    traceparent text NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    FOREIGN KEY (site_id, sku) REFERENCES inventory_stock (site_id, sku)
);
CREATE INDEX ix_reservations_due ON reservations (expires_at) WHERE status = 'ACTIVE';
CREATE INDEX ix_reservations_committed ON reservations (updated_at) WHERE status = 'COMMITTED';

-- One decision per order, whatever happens later. A replay reads this and publishes nothing.
CREATE TABLE order_outcome (
    order_id text PRIMARY KEY,
    outcome text NOT NULL CHECK (outcome IN ('RESERVED','REJECTED')),
    reservation_id text NULL REFERENCES reservations (reservation_id),
    reason_code text NULL,
    site_id text NOT NULL,
    sku text NOT NULL,
    quantity integer NOT NULL,
    event_id uuid NOT NULL,
    decided_at timestamptz NOT NULL,
    CHECK ((outcome = 'RESERVED' AND reservation_id IS NOT NULL AND reason_code IS NULL)
        OR (outcome = 'REJECTED' AND reservation_id IS NULL AND reason_code IS NOT NULL))
);

CREATE FUNCTION reject_audit_change() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION '% is immutable', TG_TABLE_NAME;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE reservation_history (
    seq bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    reservation_id text NOT NULL REFERENCES reservations (reservation_id),
    from_status text NULL,
    to_status text NOT NULL,
    actor text NOT NULL,
    reason text NOT NULL,
    event_id uuid NULL,
    occurred_at timestamptz NOT NULL
);
CREATE INDEX ix_reservation_history ON reservation_history (reservation_id, seq);
CREATE TRIGGER reservation_history_immutable BEFORE UPDATE OR DELETE ON reservation_history
    FOR EACH ROW EXECUTE FUNCTION reject_audit_change();
CREATE TRIGGER reservation_history_no_truncate BEFORE TRUNCATE ON reservation_history
    FOR EACH STATEMENT EXECUTE FUNCTION reject_audit_change();

-- Every on-hand change, including seed data and restocks.
CREATE TABLE stock_movement (
    seq bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    site_id text NOT NULL,
    sku text NOT NULL,
    delta integer NOT NULL,
    kind text NOT NULL CHECK (kind IN ('SEED','RESTOCK','SHIPMENT','LATE_SHIPMENT')),
    actor text NOT NULL,
    reason text NOT NULL,
    reference text NULL,
    idempotency_key text NULL,
    request_hash text NULL,
    occurred_at timestamptz NOT NULL,
    FOREIGN KEY (site_id, sku) REFERENCES inventory_stock (site_id, sku)
);
CREATE UNIQUE INDEX ux_stock_movement_key ON stock_movement (idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE TRIGGER stock_movement_immutable BEFORE UPDATE OR DELETE ON stock_movement
    FOR EACH ROW EXECUTE FUNCTION reject_audit_change();
CREATE TRIGGER stock_movement_no_truncate BEFORE TRUNCATE ON stock_movement
    FOR EACH STATEMENT EXECUTE FUNCTION reject_audit_change();

-- Facts Inventory cannot resolve by itself. A person reconciles them.
CREATE TABLE reconciliation_item (
    seq bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id text NOT NULL,
    reservation_id text NULL,
    kind text NOT NULL,
    stock_adjusted boolean NOT NULL DEFAULT false,
    event_id uuid NOT NULL,
    status text NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN','RESOLVED')),
    created_at timestamptz NOT NULL,
    UNIQUE (event_id, kind)
);
CREATE INDEX ix_reconciliation_open ON reconciliation_item (created_at) WHERE status = 'OPEN';

CREATE TABLE outbox (
    seq bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
    event_id uuid PRIMARY KEY,
    aggregate_id text NOT NULL,
    event_type text NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL,
    published_at timestamptz NULL,
    attempts integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL,
    claimed_by text NULL,
    claim_expires_at timestamptz NULL,
    last_error text NULL
);
CREATE INDEX ix_outbox_unpublished ON outbox (seq) WHERE published_at IS NULL;
CREATE INDEX ix_outbox_aggregate_unpublished ON outbox (aggregate_id, seq) WHERE published_at IS NULL;
-- Database-level guard: one business outcome and one expiry per order.
CREATE UNIQUE INDEX ux_outbox_once_per_order ON outbox (aggregate_id, event_type);
CREATE UNIQUE INDEX ux_outbox_one_outcome ON outbox (aggregate_id)
    WHERE event_type IN ('inventory.reserved.v1', 'inventory.rejected.v1');

CREATE TABLE processed_event (
    event_id uuid PRIMARY KEY,
    event_type text NOT NULL,
    processed_at timestamptz NOT NULL
);
