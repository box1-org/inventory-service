package org.box1.inventory;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SchemaValidatorsConfig;
import com.networknt.schema.SpecVersion;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.box1.inventory.domain.InboundEvent;
import org.box1.inventory.domain.PoisonEventException;
import org.box1.inventory.domain.ReasonCode;
import org.box1.inventory.messaging.EventParser;
import org.box1.inventory.messaging.EventTestSupport;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Validates against box1-ops-contracts at the commit pinned in pom.xml. The contracts are fetched, never copied, so
 * this repository holds no local schema. This test runs in CI, and a fetch failure fails it rather than skipping.
 */
@QuarkusTest
class ContractTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final JsonSchemaFactory FACTORY = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);

    @Inject
    EventParser parser;

    @Inject
    ObjectMapper mapper;

    private static String fetch(String path) throws IOException, InterruptedException {
        String ref = System.getProperty("contracts.ref");
        URI uri = URI.create("https://raw.githubusercontent.com/box1-org/box1-ops-contracts/" + ref + "/" + path);
        try (HttpClient client = HttpClient.newHttpClient()) {
            HttpResponse<String> response = client.send(HttpRequest.newBuilder(uri).build(), HttpResponse.BodyHandlers.ofString());
            assertEquals(200, response.statusCode(), path);
            return response.body();
        }
    }

    private static JsonSchema schema(String type) throws IOException, InterruptedException {
        SchemaValidatorsConfig config = SchemaValidatorsConfig.builder().formatAssertionsEnabled(true).build();
        return FACTORY.getSchema(fetch("events/v1/" + type + ".schema.json"), config);
    }

    @Test
    void parserAcceptsEveryConsumedFixture() throws Exception {
        Set<String> all = new TreeSet<>(EventParser.ORDER_TOPIC_TYPES);
        all.addAll(EventParser.FULFILLMENT_TOPIC_TYPES);
        Set<String> seen = new TreeSet<>();
        for (JsonNode fixture : JSON.readTree(fetch("fixtures/events.json"))) {
            String type = fixture.get("eventType").asText();
            if (all.contains(type)) {
                assertEquals(type, parser.parse(fixture.toString(), all).eventType());
                seen.add(type);
            }
        }
        assertEquals(all, seen);
    }

    @Test
    void producedEventsMatchTheirSchemasAndEnvelope() throws Exception {
        JsonSchema envelope = schema("envelope");
        Instant now = Instant.parse("2026-11-05T12:00:00Z");
        for (var event : EventTestSupport.samples(mapper, now).entrySet()) {
            JsonNode produced = event.getValue();
            assertTrue(envelope.validate(produced).isEmpty(), event.getKey() + " envelope: " + envelope.validate(produced));
            String name = event.getKey().replaceAll("\\.v[0-9]+$", "");
            JsonSchema payload = schema(name);
            assertTrue(payload.validate(produced.get("payload")).isEmpty(), event.getKey() + " payload: " + payload.validate(produced.get("payload")));
        }
    }

    /** The checks order-service main applies before accepting an inventory event, restated. */
    @Test
    void producedEventsMeetTheDeployedOrderConsumerRules() {
        Instant now = Instant.parse("2026-11-05T12:00:00Z");
        for (var event : EventTestSupport.samples(mapper, now).entrySet()) {
            JsonNode value = event.getValue();
            assertEquals(1, value.get("schemaVersion").asInt());
            assertEquals(value.get("aggregateId").asText(), value.get("payload").get("orderId").asText());
            UUID.fromString(value.get("eventId").asText());
            Instant.parse(value.get("occurredAt").asText());
            assertTrue(value.get("correlationId").asText().length() <= 128);
            assertTrue(value.get("causationId").asText().length() <= 128);
            assertTrue(value.get("payload").get("orderId").asText().matches("^ord_[A-Za-z0-9_-]{1,124}$"));
            JsonNode payload = value.get("payload");
            if (payload.has("reservationId")) {
                assertTrue(payload.get("reservationId").asText().matches("^res_[A-Za-z0-9_-]{1,124}$"));
            }
            if (payload.has("reasonCode")) {
                assertTrue(payload.get("reasonCode").asText().matches("^[A-Z][A-Z0-9_]{0,63}$"));
            }
            if (payload.has("siteId")) {
                assertTrue(payload.get("siteId").asText().matches("^site_[A-Za-z0-9_-]{1,120}$"));
            }
        }
    }

    @Test
    void reasonCodesAreTheContractSet() throws Exception {
        String doc = fetch("INVENTORY-RESERVATIONS.md");
        for (ReasonCode code : ReasonCode.values()) {
            assertTrue(doc.contains("`" + code.name() + "`"), code.name());
        }
        assertEquals(3, ReasonCode.values().length);
    }

    @Test
    void producedEventsCarryNoCustomerId() {
        for (var event : EventTestSupport.samples(mapper, Instant.now()).entrySet()) {
            assertTrue(!event.getValue().toString().contains("customerId"), event.getKey());
        }
    }

    @Test
    void malformedEventsArePoison() {
        Set<String> types = EventParser.ORDER_TOPIC_TYPES;
        String ok = Harness.placed(Harness.id(), "ord_1", "site_nyc", "SKU-42", 2);
        String[] bad = {
            "not json", "[]", "{}", null,
            ok.replace("\"schemaVersion\":1", "\"schemaVersion\":2"),
            ok.replaceFirst("\"eventId\":\"[^\"]+\"", "\"eventId\":\"not-a-uuid\""),
            ok.replace("\"aggregateId\":\"ord_1\"", "\"aggregateId\":\"ord_2\""),
            ok.replace("00-4bf9", "99-4bf9"),
            ok.replace("\"quantity\":2", "\"quantity\":0"),
            ok.replace("\"quantity\":2", "\"quantity\":2.5"),
            ok.replace("\"quantity\":2", "\"quantity\":\"2\""),
            ok.replace("\"quantity\":2", "\"quantity\":99999999999"),
            ok.replace("\"sku\":\"SKU-42\",", ""),
            ok.replace("\"customerId\":\"cust_test_secret\",", ""),
            ok.replace("site_nyc", "nyc"),
            ok.replace("order.placed.v1", "unknown.event.v1"),
            ok.replace("order.placed.v1", "shipment.confirmed.v1"),
            ok.replaceFirst("\"occurredAt\":\"[^\"]+\"", "\"occurredAt\":\"yesterday\""),
            ok.replace("\"schemaVersion\":1", "\"schemaVersion\":\"1\""),
            ok.replace("\"schemaVersion\":1", "\"schemaVersion\":1.9"),
            ok.replaceFirst("\"eventId\":\"[^\"]+\"", "\"eventId\":\"1-1-1-1-1\""),
            ok.replace("corr-test", "corr\\u0000test"),
            ok.replace("cause-test", "c".repeat(129)),
        };
        for (String value : bad) {
            Assertions.assertThrows(PoisonEventException.class, () -> parser.parse(value, types), value);
        }
        assertEquals(InboundEvent.ORDER_PLACED, parser.parse(ok.replace("\"siteId\"", "\"futureField\":true,\"siteId\""), types).eventType(),
                "an unknown field is an additive change");
    }

    @Test
    void generatedOpenApiCoversTheContract() throws Exception {
        JsonNode generated = JSON.readTree(given().accept("application/json").when().get("/q/openapi").asString());
        JsonNode contract = new YAMLMapper().readTree(fetch("openapi/inventory-service.yaml"));
        int operations = 0;
        for (var path : (Iterable<Map.Entry<String, JsonNode>>) contract.get("paths")::fields) {
            for (var op : (Iterable<Map.Entry<String, JsonNode>>) path.getValue()::fields) {
                assertEquals(op.getValue().get("operationId").asText(),
                        generated.path("paths").path(path.getKey()).path(op.getKey()).path("operationId").asText(),
                        path.getKey() + " " + op.getKey());
                JsonNode served = generated.path("paths").path(path.getKey()).path(op.getKey()).path("responses");
                Set<String> expected = new TreeSet<>();
                op.getValue().get("responses").fieldNames().forEachRemaining(expected::add);
                Set<String> actual = new TreeSet<>();
                served.fieldNames().forEachRemaining(actual::add);
                assertEquals(expected, actual, "responses of " + path.getKey() + " " + op.getKey());
                for (String code : expected) {
                    if (code.startsWith("4")) {
                        assertTrue(served.path(code).path("content").has("application/problem+json"), code + " of " + path.getKey() + " " + op.getKey());
                    }
                }
                operations++;
            }
        }
        assertEquals(4, operations);
    }
}
