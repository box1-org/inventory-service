package org.box1.inventory;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.startsWith;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Stops PostgreSQL. Runs in its own Quarkus instance so it cannot affect other scenarios. */
@QuarkusTest
@Tag("containers")
@TestProfile(DatabaseDownTest.OwnStack.class)
@QuarkusTestResource(value = FullStackResource.class, restrictToAnnotatedClass = true)
class DatabaseDownTest {
    /** A distinct profile forces a separate application start. */
    public static class OwnStack implements QuarkusTestProfile {}

    @Test
    void readinessFailsWhileLivenessAndHonestErrorsHold() {
        given().when().get("/q/health/ready").then().statusCode(200);
        FullStackResource.postgres.stop();
        Harness.await("readiness to fail", () -> given().when().get("/q/health/ready").statusCode() == 503);
        given().when().get("/q/health/live").then().statusCode(200);
        given().when().get("/api/v1/inventory").then().statusCode(503).contentType(startsWith("application/problem+json"));
        given().when().get("/api/greetings").then().statusCode(200);
    }
}
