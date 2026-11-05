package org.box1.inventory;

import java.util.Map;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;

/** Starts PostgreSQL and Kafka for tagged tests and switches the service to its production behavior. */
public class FullStackResource implements QuarkusTestResourceLifecycleManager {
    static PostgreSQLContainer<?> postgres;
    static KafkaContainer kafka;

    @Override
    public Map<String, String> start() {
        postgres = Containers.postgres();
        kafka = Containers.kafka();
        postgres.start();
        kafka.start();
        return Map.ofEntries(
                Map.entry("quarkus.datasource.jdbc.url", postgres.getJdbcUrl()),
                Map.entry("quarkus.datasource.username", postgres.getUsername()),
                Map.entry("quarkus.datasource.password", postgres.getPassword()),
                Map.entry("inventory.readiness.database", "true"),
                Map.entry("quarkus.flyway.migrate-at-start", "true"),
                Map.entry("quarkus.scheduler.enabled", "true"),
                Map.entry("kafka.bootstrap.servers", kafka.getBootstrapServers()),
                Map.entry("inventory.consumer.order-events-floor", Harness.FLOOR),
                Map.entry("inventory.security.read-policy", "permit"),
                Map.entry("inventory.outbox.poll-interval", "200ms"),
                Map.entry("inventory.outbox.lease-seconds", "3"),
                Map.entry("inventory.outbox.backoff-base-seconds", "0.2"),
                Map.entry("inventory.outbox.backoff-max-seconds", "1"),
                Map.entry("inventory.consumer.initial-backoff-millis", "20"),
                Map.entry("inventory.consumer.max-attempts", "5"),
                Map.entry("inventory.expiry.interval", "200ms"),
                Map.entry("inventory.expiry.grace", "PT0S"),
                Map.entry("inventory.metrics.refresh-interval", "1s"));
    }

    @Override
    public void stop() {
        if (kafka != null) {
            kafka.stop();
        }
        if (postgres != null) {
            postgres.stop();
        }
    }
}
