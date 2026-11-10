-- Site events from box1.site.events.v1 (box1-docs#129; SITE.md in box1-ops-contracts). Additive only: no existing row
-- is updated or deleted, every existing movement kind and reconciliation kind stays valid, and the order flow tables are
-- untouched.

-- COUNT is the on-hand change made by a site cycle count. The kinds from V1 and V3 all stay.
ALTER TABLE stock_movement DROP CONSTRAINT stock_movement_kind_check;
ALTER TABLE stock_movement ADD CONSTRAINT stock_movement_kind_check
    CHECK (kind IN ('SEED','RESTOCK','SHIPMENT','LATE_SHIPMENT','OPENING','COUNT'));

-- When the movement happened in the business, as opposed to when this service recorded it (occurred_at). A shipment
-- carries shippedAt and a count its countedAt. NULL means the same as occurred_at: rows written before V4, seeds,
-- openings and restocks. A nullable column with no default rewrites no row, so the immutability triggers do not fire.
ALTER TABLE stock_movement ADD COLUMN business_at timestamptz NULL;
-- The count rule sums a site and SKU's movements after a time.
CREATE INDEX ix_stock_movement_site_sku ON stock_movement (site_id, sku);

-- Site picks. A pick never changes on_hand or reserved: shipment.confirmed.v1 already does.
CREATE TABLE site_pick (
    pick_id text PRIMARY KEY,
    event_id uuid NOT NULL UNIQUE,
    site_id text NOT NULL,
    order_id text NOT NULL,
    reservation_id text NOT NULL,
    sku text NOT NULL,
    quantity integer NOT NULL CHECK (quantity > 0),
    picked_at timestamptz NOT NULL,
    outcome text NOT NULL CHECK (outcome IN ('PICKED','PICKED_AFTER_SHIPMENT','NOT_HELD','MISMATCH')),
    -- The reservation status the decision saw; NULL when the reservation is unknown.
    reservation_status text NULL,
    recorded_at timestamptz NOT NULL
);
CREATE INDEX ix_site_pick_site ON site_pick (site_id, recorded_at);
CREATE INDEX ix_site_pick_reservation ON site_pick (reservation_id);

-- Site cycle counts, applied, superseded by a later count, or for stock this service does not have.
CREATE TABLE site_count (
    count_id text PRIMARY KEY,
    event_id uuid NOT NULL UNIQUE,
    site_id text NOT NULL,
    sku text NOT NULL,
    counted_quantity integer NOT NULL CHECK (counted_quantity >= 0),
    counted_at timestamptz NOT NULL,
    reason text NOT NULL,
    outcome text NOT NULL CHECK (outcome IN ('APPLIED','SUPERSEDED','UNKNOWN_STOCK')),
    -- For APPLIED: E, the later movements D, reserved, and on_hand before and after. NULL otherwise.
    expected integer NULL,
    later_movements integer NULL,
    reserved integer NULL,
    on_hand_before integer NULL,
    on_hand_after integer NULL,
    recorded_at timestamptz NOT NULL,
    CHECK (outcome <> 'APPLIED' OR (expected IS NOT NULL AND later_movements IS NOT NULL AND reserved IS NOT NULL
        AND on_hand_before IS NOT NULL AND on_hand_after IS NOT NULL))
);
CREATE INDEX ix_site_count_applied ON site_count (site_id, sku, counted_at, count_id) WHERE outcome = 'APPLIED';
CREATE INDEX ix_site_count_site ON site_count (site_id, recorded_at);

-- Site holds, one row per transition. In v1 a hold changes neither on_hand nor available.
CREATE TABLE site_hold (
    hold_id text NOT NULL,
    action text NOT NULL CHECK (action IN ('PLACED','RELEASED')),
    event_id uuid NOT NULL UNIQUE,
    site_id text NOT NULL,
    sku text NOT NULL,
    quantity integer NOT NULL CHECK (quantity > 0),
    reason_code text NOT NULL,
    changed_at timestamptz NOT NULL,
    outcome text NOT NULL CHECK (outcome IN ('RECORDED','UNKNOWN_HOLD')),
    recorded_at timestamptz NOT NULL,
    PRIMARY KEY (hold_id, action)
);
CREATE INDEX ix_site_hold_site ON site_hold (site_id, recorded_at);

-- The site records are audit: like stock_movement, they never change once written.
CREATE TRIGGER site_pick_immutable BEFORE UPDATE OR DELETE ON site_pick
    FOR EACH ROW EXECUTE FUNCTION reject_audit_change();
CREATE TRIGGER site_pick_no_truncate BEFORE TRUNCATE ON site_pick
    FOR EACH STATEMENT EXECUTE FUNCTION reject_audit_change();
CREATE TRIGGER site_count_immutable BEFORE UPDATE OR DELETE ON site_count
    FOR EACH ROW EXECUTE FUNCTION reject_audit_change();
CREATE TRIGGER site_count_no_truncate BEFORE TRUNCATE ON site_count
    FOR EACH STATEMENT EXECUTE FUNCTION reject_audit_change();
CREATE TRIGGER site_hold_immutable BEFORE UPDATE OR DELETE ON site_hold
    FOR EACH ROW EXECUTE FUNCTION reject_audit_change();
CREATE TRIGGER site_hold_no_truncate BEFORE TRUNCATE ON site_hold
    FOR EACH STATEMENT EXECUTE FUNCTION reject_audit_change();

-- Reconciliation items from site events. Count and hold items belong to a site and SKU rather than an order, so
-- order_id may now be NULL, but only for those kinds: every kind that existed before V4 still needs an order.
ALTER TABLE reconciliation_item ALTER COLUMN order_id DROP NOT NULL;
ALTER TABLE reconciliation_item ADD CONSTRAINT reconciliation_item_order_required
    CHECK (order_id IS NOT NULL OR kind IN ('COUNT_CONFLICT','COUNT_UNKNOWN_STOCK','HOLD_UNKNOWN'));
ALTER TABLE reconciliation_item ADD COLUMN site_id text NULL;
ALTER TABLE reconciliation_item ADD COLUMN sku text NULL;
-- What an operator needs to decide, for example E, reserved and the shortfall of a COUNT_CONFLICT.
ALTER TABLE reconciliation_item ADD COLUMN detail jsonb NULL;
