package org.box1.inventory;

import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/** Real PostgreSQL and Kafka matching the deployed major versions. */
final class Containers {
    static final String POSTGRES_IMAGE = "docker.io/library/postgres:15-alpine";
    static final String KAFKA_IMAGE = "docker.io/apache/kafka:4.2.0";

    private Containers() {}

    @SuppressWarnings("resource")
    static PostgreSQLContainer<?> postgres() {
        return new PostgreSQLContainer<>(DockerImageName.parse(POSTGRES_IMAGE).asCompatibleSubstituteFor("postgres"))
                .withDatabaseName("inventory_service").withUsername("inventory_service");
    }

    static KafkaContainer kafka() {
        return new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE).asCompatibleSubstituteFor("apache/kafka"));
    }
}
