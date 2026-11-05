package org.box1.inventory;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Readiness;

/** PostgreSQL is a readiness dependency: without it the service can answer nothing. Kafka is deliberately not. */
@Readiness
@ApplicationScoped
public class DatabaseReadinessCheck implements HealthCheck {
    private static final int VALIDATION_SECONDS = 2;

    private final DataSource dataSource;
    private final boolean enabled;

    public DatabaseReadinessCheck(DataSource dataSource, @ConfigProperty(name = "inventory.readiness.database") boolean enabled) {
        this.dataSource = dataSource;
        this.enabled = enabled;
    }

    @Override
    public HealthCheckResponse call() {
        if (!enabled) {
            return HealthCheckResponse.named("database").up().withData("check", "disabled").build();
        }
        try (Connection connection = dataSource.getConnection()) {
            return HealthCheckResponse.named("database").status(connection.isValid(VALIDATION_SECONDS)).build();
        } catch (SQLException error) {
            return HealthCheckResponse.named("database").down().withData("sqlState", String.valueOf(error.getSQLState())).build();
        }
    }
}
