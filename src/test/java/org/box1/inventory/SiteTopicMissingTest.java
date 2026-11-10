package org.box1.inventory;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The site topic does not exist yet, as before box1-gitops creates it. The pod must stay live and Ready, must not make
 * the broker create the topic, and must start consuming once the topic exists. Own application and own broker.
 */
@QuarkusTest
@Tag("containers")
@TestProfile(SiteTopicMissingTest.OwnStack.class)
@QuarkusTestResource(value = FullStackResource.class, restrictToAnnotatedClass = true)
class SiteTopicMissingTest {
    /** A distinct profile forces a separate application start. */
    public static class OwnStack implements QuarkusTestProfile {}

    private static Admin admin() {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, FullStackResource.kafka.getBootstrapServers());
        return Admin.create(config);
    }

    @Test
    void aMissingSiteTopicLeavesTheServiceReadyAndIsPickedUpOnceCreated() throws Exception {
        long until = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (System.nanoTime() < until) {
            given().when().get("/q/health/ready").then().statusCode(200);
            given().when().get("/q/health/live").then().statusCode(200);
            Thread.sleep(1000);
        }
        try (Admin admin = admin()) {
            assertFalse(admin.listTopics().names().get().contains(Harness.SITE_TOPIC), "the consumer did not create the topic");
            admin.createTopics(List.of(new NewTopic(Harness.SITE_TOPIC, 3, (short) 1))).all().get();
        }
        String sku = Harness.newSku("site_nyc", 5);
        String eventId = Harness.id();
        Harness.sendSite("site_nyc", Harness.count(eventId, "site_nyc", "cnt_missing_topic", sku, 4, Instant.now()));
        Harness.await("applied after the topic appears", Duration.ofMinutes(6), () -> Harness.onHand("site_nyc", sku) == 4);
        assertEquals(1, Harness.count("SELECT count(*) FROM processed_event WHERE event_id = ?::uuid", eventId));
        given().when().get("/q/health/ready").then().statusCode(200);
        given().when().get("/q/health/live").then().statusCode(200);
    }
}
