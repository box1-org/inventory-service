package org.box1.inventory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.output.MigrateResult;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

/** Migrations on an empty database, and a rerun, without the application. */
@Tag("containers")
class MigrationTest {
    @Test
    void migratesEmptyThenRerunsAsNoOpAndKeepsNoCredentials() throws Exception {
        try (PostgreSQLContainer<?> postgres = Containers.postgres()) {
            postgres.start();
            Flyway flyway = Flyway.configure().dataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())
                    .locations("classpath:db/migration").load();
            MigrateResult first = flyway.migrate();
            assertEquals(4, first.migrationsExecuted);
            assertEquals(0, flyway.migrate().migrationsExecuted, "rerun is a no-op");
            try (Connection c = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
                    Statement s = c.createStatement()) {
                try (ResultSet r = s.executeQuery("SELECT count(*) FROM inventory_stock WHERE reserved <> 0")) {
                    r.next();
                    assertEquals(0, r.getInt(1));
                }
                try (ResultSet r = s.executeQuery("SELECT count(*) FROM stock_movement WHERE kind = 'SEED'")) {
                    r.next();
                    assertEquals(5, r.getInt(1), "the seed is attributable");
                }
                try (ResultSet r = s.executeQuery("SELECT count(*) FROM pg_indexes WHERE indexname = 'ix_reservations_due'")) {
                    r.next();
                    assertEquals(1, r.getInt(1));
                }
                assertThrows(java.sql.SQLException.class, () -> s.execute(
                        "INSERT INTO stock_movement (site_id, sku, delta, kind, actor, reason, occurred_at)"
                                + " VALUES ('site_nyc', 'SKU-42', 1, 'BOGUS', 'a', 'r', now())"), "movement kinds stay constrained");
                try (ResultSet r = s.executeQuery("SELECT count(*) FROM inventory_site")) {
                    r.next();
                    assertEquals(3, r.getInt(1), "V3 adds no sites");
                }
                assertThrows(java.sql.SQLException.class, () -> s.execute(
                        "INSERT INTO reservations (reservation_id, order_id, site_id, sku, quantity, status, expires_at, placed_event_id, correlation_id, created_at, updated_at)"
                                + " VALUES ('res_x', 'ord_x', 'site_nyc', 'SKU-42', 1, 'BOGUS', now(), gen_random_uuid(), 'c', now(), now())"));
            }
        }
    }

    /**
     * V4 against a database that already holds V1 to V3 data, as the dev database does: every existing row stays as it
     * was, every earlier movement kind and reconciliation kind is still accepted, and the new rules hold.
     */
    @Test
    void v4IsAdditiveOnAPopulatedDatabase() throws Exception {
        try (PostgreSQLContainer<?> postgres = Containers.postgres()) {
            postgres.start();
            Flyway upToV3 = Flyway.configure().dataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())
                    .locations("classpath:db/migration").target("3").load();
            assertEquals(3, upToV3.migrate().migrationsExecuted);
            try (Connection c = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
                    Statement s = c.createStatement()) {
                s.execute("UPDATE inventory_stock SET reserved = 2 WHERE site_id = 'site_nyc' AND sku = 'SKU-42'");
                s.execute("INSERT INTO reservations (reservation_id, order_id, site_id, sku, quantity, status, expires_at, placed_event_id,"
                        + " correlation_id, created_at, updated_at) VALUES ('res_old', 'ord_old', 'site_nyc', 'SKU-42', 2, 'ACTIVE', now(),"
                        + " gen_random_uuid(), 'c', now(), now())");
                for (String kind : new String[] {"RESTOCK", "SHIPMENT", "LATE_SHIPMENT"}) {
                    s.execute("INSERT INTO stock_movement (site_id, sku, delta, kind, actor, reason, occurred_at)"
                            + " VALUES ('site_nyc', 'SKU-42', 0, '" + kind + "', 'a', 'r', now())");
                }
                s.execute("INSERT INTO reconciliation_item (order_id, reservation_id, kind, event_id, created_at)"
                        + " VALUES ('ord_old', 'res_old', 'SHIPMENT_FAILED', gen_random_uuid(), now())");
                s.execute("INSERT INTO outbox (event_id, aggregate_id, event_type, payload, created_at, next_attempt_at)"
                        + " VALUES (gen_random_uuid(), 'ord_old', 'inventory.reserved.v1', '{}'::jsonb, now(), now())");
            }
            String before = snapshot(postgres);
            Flyway all = Flyway.configure().dataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())
                    .locations("classpath:db/migration").load();
            assertEquals(1, all.migrate().migrationsExecuted, "only V4 runs");
            assertEquals(before, snapshot(postgres), "V4 changes no existing row");
            try (Connection c = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
                    Statement s = c.createStatement()) {
                for (String kind : new String[] {"SEED", "RESTOCK", "SHIPMENT", "LATE_SHIPMENT", "COUNT"}) {
                    s.execute("INSERT INTO stock_movement (site_id, sku, delta, kind, actor, reason, occurred_at)"
                            + " VALUES ('site_bos', 'SKU-42', 0, '" + kind + "', 'a', 'r', now())");
                }
                assertThrows(java.sql.SQLException.class, () -> s.execute("INSERT INTO stock_movement (site_id, sku, delta, kind, actor,"
                        + " reason, occurred_at) VALUES ('site_bos', 'SKU-42', 0, 'PICK', 'a', 'r', now())"), "kinds stay closed");
                assertThrows(java.sql.SQLException.class, () -> s.execute("UPDATE stock_movement SET business_at = now()"),
                        "movements stay immutable");
                assertThrows(java.sql.SQLException.class, () -> s.execute("INSERT INTO reconciliation_item (order_id, kind, event_id,"
                        + " created_at) VALUES (NULL, 'SHIPMENT_FAILED', gen_random_uuid(), now())"), "order kinds still need an order");
                s.execute("INSERT INTO reconciliation_item (order_id, kind, site_id, sku, detail, event_id, created_at)"
                        + " VALUES (NULL, 'COUNT_CONFLICT', 'site_nyc', 'SKU-42', '{\"shortfall\":1}', gen_random_uuid(), now())");
                s.execute("INSERT INTO site_pick (pick_id, event_id, site_id, order_id, reservation_id, sku, quantity, picked_at, outcome,"
                        + " recorded_at) VALUES ('pick_1', gen_random_uuid(), 'site_nyc', 'ord_old', 'res_old', 'SKU-42', 2, now(), 'PICKED', now())");
                assertThrows(java.sql.SQLException.class, () -> s.execute("INSERT INTO site_pick (pick_id, event_id, site_id, order_id,"
                        + " reservation_id, sku, quantity, picked_at, outcome, recorded_at) VALUES ('pick_1', gen_random_uuid(), 'site_nyc',"
                        + " 'ord_old', 'res_old', 'SKU-42', 2, now(), 'PICKED', now())"), "pickId is unique");
                assertThrows(java.sql.SQLException.class, () -> s.execute("DELETE FROM site_pick"), "site records are immutable");
                s.execute("INSERT INTO site_hold (hold_id, action, event_id, site_id, sku, quantity, reason_code, changed_at, outcome,"
                        + " recorded_at) VALUES ('hold_1', 'PLACED', gen_random_uuid(), 'site_nyc', 'SKU-42', 1, 'DAMAGED', now(), 'RECORDED', now())");
                assertThrows(java.sql.SQLException.class, () -> s.execute("INSERT INTO site_hold (hold_id, action, event_id, site_id, sku,"
                        + " quantity, reason_code, changed_at, outcome, recorded_at) VALUES ('hold_1', 'PLACED', gen_random_uuid(), 'site_nyc',"
                        + " 'SKU-42', 1, 'DAMAGED', now(), 'RECORDED', now())"), "(hold_id, action) is unique");
                assertThrows(java.sql.SQLException.class, () -> s.execute("INSERT INTO site_count (count_id, event_id, site_id, sku,"
                        + " counted_quantity, counted_at, reason, outcome, recorded_at) VALUES ('cnt_1', gen_random_uuid(), 'site_nyc',"
                        + " 'SKU-42', 1, now(), 'CYCLE_COUNT', 'APPLIED', now())"), "an applied count records the rule's values");
            }
        }
    }

    /** Every pre-V4 row of the tables V4 touches or sits beside, as text. */
    private static String snapshot(PostgreSQLContainer<?> postgres) throws Exception {
        StringBuilder out = new StringBuilder();
        try (Connection c = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
                Statement s = c.createStatement()) {
            for (String sql : new String[] {
                "SELECT site_id, sku, on_hand, reserved, updated_at FROM inventory_stock ORDER BY 1, 2",
                "SELECT reservation_id, status, quantity FROM reservations ORDER BY 1",
                "SELECT seq, site_id, sku, delta, kind, occurred_at FROM stock_movement ORDER BY 1",
                "SELECT seq, order_id, kind, status, event_id FROM reconciliation_item ORDER BY 1",
                "SELECT aggregate_id, event_type, published_at FROM outbox ORDER BY 1, 2",
                "SELECT site_id, status FROM inventory_site ORDER BY 1"}) {
                try (ResultSet r = s.executeQuery(sql)) {
                    int columns = r.getMetaData().getColumnCount();
                    while (r.next()) {
                        for (int i = 1; i <= columns; i++) {
                            out.append(r.getString(i)).append('|');
                        }
                        out.append('\n');
                    }
                }
            }
        }
        return out.toString();
    }
}
