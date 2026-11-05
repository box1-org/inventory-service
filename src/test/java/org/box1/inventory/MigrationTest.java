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
            assertEquals(2, first.migrationsExecuted);
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
                        "INSERT INTO reservations (reservation_id, order_id, site_id, sku, quantity, status, expires_at, placed_event_id, correlation_id, created_at, updated_at)"
                                + " VALUES ('res_x', 'ord_x', 'site_nyc', 'SKU-42', 1, 'BOGUS', now(), gen_random_uuid(), 'c', now(), now())"));
            }
        }
    }
}
