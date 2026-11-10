package org.box1.inventory;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

/** Access rules that hold without a database, with the image's default read policy. Runs in CI. */
@QuarkusTest
@QuarkusTestResource(TestTokens.class)
class SecurityTest {
    private static final String RESTOCK = "/internal/v1/stock/site_nyc/SKU-42/restock";
    private static final String RELEASE = "/internal/v1/reservations/res_1/release";
    private static final String BODY = "{\"quantity\":1,\"reason\":\"test\"}";
    private static final String[] READS = {"/api/v1/inventory", "/api/v1/reservations/res_1", "/api/v1/reconciliation-items",
        "/api/v1/site-events"};

    private static void rejected(int status, String token, String path) {
        var request = given().contentType("application/json").header("Idempotency-Key", "k").body(BODY);
        if (token != null) {
            request.header("Authorization", "Bearer " + token);
        }
        request.when().post(path).then().statusCode(status).contentType(startsWith("application/problem+json"));
    }

    private static final String SITE = "/internal/v1/sites/site_test";
    private static final String STOCK = "/internal/v1/stock/site_test/SKU-1";

    private static void rejectedPut(int status, String token, String path, String body) {
        var request = given().contentType("application/json").body(body);
        if (token != null) {
            request.header("Authorization", "Bearer " + token);
        }
        request.when().put(path).then().statusCode(status).contentType(startsWith("application/problem+json"));
    }

    @Test
    void creationRoutesNeedTheRole() {
        String site = "{\"status\":\"ACTIVE\",\"reason\":\"test\"}";
        String stock = "{\"openingQuantity\":1,\"reason\":\"test\"}";
        rejectedPut(401, null, SITE, site);
        rejectedPut(401, null, STOCK, stock);
        rejectedPut(401, TestTokens.wrongAudience(), SITE, site);
        rejectedPut(403, TestTokens.viewer(), SITE, site);
        rejectedPut(403, TestTokens.viewer(), STOCK, stock);
        rejectedPut(403, TestTokens.groupsOnly(), STOCK, stock);
        String admin = TestTokens.admin("subject-1");
        rejectedPut(400, admin, SITE, "{\"status\":\"OPEN\",\"reason\":\"x\"}");
        rejectedPut(400, admin, "/internal/v1/sites/nyc", site);
        rejectedPut(400, admin, STOCK, "{\"openingQuantity\":-1,\"reason\":\"x\"}");
        rejectedPut(400, admin, STOCK, "{\"openingQuantity\":1000001,\"reason\":\"x\"}");
        rejectedPut(400, admin, STOCK, "{\"openingQuantity\":1}");
        rejectedPut(400, admin, STOCK, "{\"openingQuantity\":1,\"reason\":\"x\",\"onHand\":5}");
    }

    @Test
    void anonymousMutationIsRejected() {
        rejected(401, null, RESTOCK);
        rejected(401, null, RELEASE);
    }

    @Test
    void badTokensAreRejected() {
        for (String token : new String[] {"not-a-token", TestTokens.expired(), TestTokens.wrongIssuer(),
                TestTokens.wrongAudience(), TestTokens.wrongKey()}) {
            rejected(401, token, RESTOCK);
            rejected(401, token, RELEASE);
        }
    }

    @Test
    void aTokenWithoutTheRoleIsForbidden() {
        rejected(403, TestTokens.viewer(), RESTOCK);
        rejected(403, TestTokens.viewer(), RELEASE);
    }

    @Test
    void theRoleReachesValidation() {
        given().contentType("application/json").header("Authorization", "Bearer " + TestTokens.admin("subject-1"))
                .body("{\"quantity\":0,\"reason\":\"x\"}").header("Idempotency-Key", "k")
                .when().post(RESTOCK).then().statusCode(400).contentType(startsWith("application/problem+json"));
        given().contentType("application/json").header("Authorization", "Bearer " + TestTokens.admin("subject-1"))
                .body("{\"quantity\":1,\"reason\":\"x\"}")
                .when().post(RESTOCK).then().statusCode(400).body("detail", containsString("Idempotency-Key"));
        given().contentType("application/json").header("Authorization", "Bearer " + TestTokens.admin("subject-1"))
                .body("{\"quantity\":\"2\",\"reason\":\"x\"}").header("Idempotency-Key", "k")
                .when().post(RESTOCK).then().statusCode(400).contentType(startsWith("application/problem+json"));
        given().contentType("application/json").header("Authorization", "Bearer " + TestTokens.admin("subject-1"))
                .body("{\"reason\":\"x\",\"unexpected\":1}")
                .when().post(RELEASE).then().statusCode(400).contentType(startsWith("application/problem+json"));
    }

    @Test
    void aRoleOutsideTheClientClaimIsForbidden() {
        rejected(403, TestTokens.groupsOnly(), RESTOCK);
        rejected(403, TestTokens.groupsOnly(), RELEASE);
    }

    @Test
    void anInvalidTokenOnAReadRouteIsRejected() {
        for (String token : new String[] {"not-a-token", TestTokens.expired(), TestTokens.wrongKey()}) {
            for (String path : READS) {
                given().header("Authorization", "Bearer " + token).when().get(path).then().statusCode(401)
                        .contentType(startsWith("application/problem+json"));
            }
        }
    }

    @Test
    void readsFailClosedByDefault() {
        for (String path : READS) {
            given().when().get(path).then().statusCode(401).contentType(startsWith("application/problem+json"));
        }
    }

    @Test
    void siteReadFiltersAreValidatedBeforeAnyQuery() {
        String viewer = TestTokens.viewer();
        for (String path : new String[] {"/api/v1/reconciliation-items?status=CLOSED", "/api/v1/reconciliation-items?kind=bad",
            "/api/v1/reconciliation-items?siteId=nyc", "/api/v1/reconciliation-items?orderId=1042",
            "/api/v1/reconciliation-items?limit=101", "/api/v1/site-events?type=SHIP", "/api/v1/site-events?siteId=ds9",
            "/api/v1/site-events?offset=-1"}) {
            given().header("Authorization", "Bearer " + viewer).when().get(path).then().statusCode(400)
                    .contentType(startsWith("application/problem+json"));
        }
    }

    @Test
    void readRoutesExposeNoMutation() {
        for (String path : READS) {
            given().contentType("application/json").body("{}").when().post(path).then().statusCode(405);
            given().contentType("application/json").body("{}").when().put(path).then().statusCode(405);
            given().when().delete(path).then().statusCode(405);
            given().contentType("application/json").body("{}").when().patch(path).then().statusCode(405);
        }
    }

    @Test
    void platformPathsStayOpenForTheSmokeTask() {
        given().when().get("/api/greetings?name=Ada").then().statusCode(200);
        given().when().get("/api/info").then().statusCode(200).body("name", is("inventory-service"));
        given().when().get("/q/health/live").then().statusCode(200);
        given().when().get("/q/openapi").then().statusCode(200);
        given().when().get("/q/metrics").then().statusCode(200);
    }

    @Test
    void noOtherInternalRouteExists() {
        given().when().get("/internal/v1/stock").then().statusCode(404);
        given().when().get("/internal/v1/replay").then().statusCode(404);
        given().when().post("/api/v1/inventory/restock").then().statusCode(404);
        given().when().get("/q/dev").then().statusCode(404);
    }
}
