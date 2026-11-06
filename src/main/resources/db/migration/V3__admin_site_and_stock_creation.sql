-- Sites and stock records created through the admin API (box1-docs#19). Additive only: the API never updates or
-- deletes a site or a stock record, so the V2 lab fixture and every existing row stay as they are.
CREATE TABLE site_registration (
    site_id text PRIMARY KEY REFERENCES inventory_site (site_id),
    status text NOT NULL CHECK (status IN ('ACTIVE','UNAVAILABLE')),
    actor text NOT NULL,
    reason text NOT NULL,
    occurred_at timestamptz NOT NULL
);
CREATE TRIGGER site_registration_immutable BEFORE UPDATE OR DELETE ON site_registration
    FOR EACH ROW EXECUTE FUNCTION reject_audit_change();
CREATE TRIGGER site_registration_no_truncate BEFORE TRUNCATE ON site_registration
    FOR EACH STATEMENT EXECUTE FUNCTION reject_audit_change();

-- OPENING is the first on-hand quantity of a stock record created through the API. At most one per record.
ALTER TABLE stock_movement DROP CONSTRAINT stock_movement_kind_check;
ALTER TABLE stock_movement ADD CONSTRAINT stock_movement_kind_check
    CHECK (kind IN ('SEED','RESTOCK','SHIPMENT','LATE_SHIPMENT','OPENING'));
CREATE UNIQUE INDEX ux_stock_movement_one_opening ON stock_movement (site_id, sku) WHERE kind = 'OPENING';
