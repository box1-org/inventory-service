-- Lab fixture, reviewed in Git and recorded in flyway_schema_history. No route loads this data.
INSERT INTO inventory_site (site_id, status) VALUES
    ('site_nyc', 'ACTIVE'),
    ('site_bos', 'ACTIVE'),
    ('site_closed', 'UNAVAILABLE');

INSERT INTO inventory_stock (site_id, sku, on_hand, updated_at) VALUES
    ('site_nyc', 'SKU-42', 100, now()),
    ('site_nyc', 'SKU-7', 25, now()),
    ('site_nyc', 'SKU-LAST', 1, now()),
    ('site_bos', 'SKU-42', 10, now()),
    ('site_closed', 'SKU-42', 50, now());

INSERT INTO stock_movement (site_id, sku, delta, kind, actor, reason, occurred_at)
SELECT site_id, sku, on_hand, 'SEED', 'flyway', 'Lab fixture V2', now() FROM inventory_stock;
