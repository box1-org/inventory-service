package org.box1.inventory.api;

import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import org.box1.inventory.store.SiteStore;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;

/**
 * Operator reads of reconciliation items and recorded site decisions (box1-docs#129). Read-only, under the same read
 * policy as the other /api/v1 reads. Not part of the box1-ops-contracts OpenAPI yet; served in this service's own.
 */
@Path("/api/v1")
@Produces(MediaType.APPLICATION_JSON)
public class ReconciliationResource {
    private static final int DEFAULT_LIMIT = 50;
    private static final int MAX_LIMIT = 100;
    private static final Pattern KIND = Pattern.compile("^[A-Z][A-Z_]{0,63}$");
    private static final Pattern ORDER = Pattern.compile("^ord_[A-Za-z0-9_-]{1,124}$");
    private static final Set<String> STATUSES = Set.of("OPEN", "RESOLVED");
    private static final Set<String> TYPES = Set.of("PICK", "COUNT", "HOLD");

    private final SiteStore store;
    private final ReadAccess access;
    private final ObjectMapper mapper;

    public ReconciliationResource(SiteStore store, ReadAccess access, ObjectMapper mapper) {
        this.store = store;
        this.access = access;
        this.mapper = mapper;
    }

    @GET
    @Path("/reconciliation-items")
    @Operation(operationId = "listReconciliationItems",
            summary = "Reconciliation items, newest first: order-flow kinds and the site kinds PICK_NOT_HELD, PICK_MISMATCH, "
                    + "COUNT_CONFLICT, COUNT_UNKNOWN_STOCK and HOLD_UNKNOWN")
    @APIResponse(responseCode = "200", description = "Reconciliation items ordered by seq, newest first",
            content = @Content(mediaType = MediaType.APPLICATION_JSON,
                    schema = @Schema(type = SchemaType.ARRAY, implementation = Views.ReconciliationItem.class)))
    @APIResponse(responseCode = "400", description = "Invalid filter or page", content = @Content(mediaType = Problem.MEDIA_TYPE,
            schema = @Schema(implementation = Problem.class)))
    @APIResponse(responseCode = "401", description = "A token is required by the read policy", content = @Content(mediaType = Problem.MEDIA_TYPE,
            schema = @Schema(implementation = Problem.class)))
    public List<Views.ReconciliationItem> reconciliationItems(
            @Parameter(schema = @Schema(enumeration = {"OPEN", "RESOLVED"})) @QueryParam("status") String status,
            @QueryParam("kind") String kind, @QueryParam("siteId") String siteId, @QueryParam("orderId") String orderId,
            @QueryParam("limit") String limit, @QueryParam("offset") String offset) {
        access.check();
        if (status != null && !STATUSES.contains(status)) {
            throw Validation.badRequest("status must be OPEN or RESOLVED");
        }
        if (kind != null && !KIND.matcher(kind).matches()) {
            throw Validation.badRequest("kind is invalid");
        }
        if (siteId != null) {
            Validation.site(siteId);
        }
        if (orderId != null && !ORDER.matcher(orderId).matches()) {
            throw Validation.badRequest("orderId is invalid");
        }
        int size = Validation.bounded(limit, "limit", DEFAULT_LIMIT, 1, MAX_LIMIT);
        int start = Validation.bounded(offset, "offset", 0, 0, Integer.MAX_VALUE);
        return store.reconciliationItems(status, kind, siteId, orderId, size, start).stream()
                .map(row -> Views.ReconciliationItem.of(row, detail(row.detailJson()))).toList();
    }

    @GET
    @Path("/site-events")
    @Operation(operationId = "listSiteEvents",
            summary = "Recorded site picks, cycle counts and holds with the decision taken for each, newest first")
    @APIResponse(responseCode = "200", description = "Site decisions ordered by recordedAt, newest first",
            content = @Content(mediaType = MediaType.APPLICATION_JSON,
                    schema = @Schema(type = SchemaType.ARRAY, implementation = Views.SiteDecision.class)))
    @APIResponse(responseCode = "400", description = "Invalid filter or page", content = @Content(mediaType = Problem.MEDIA_TYPE,
            schema = @Schema(implementation = Problem.class)))
    @APIResponse(responseCode = "401", description = "A token is required by the read policy", content = @Content(mediaType = Problem.MEDIA_TYPE,
            schema = @Schema(implementation = Problem.class)))
    public List<Views.SiteDecision> siteEvents(@QueryParam("siteId") String siteId,
            @Parameter(schema = @Schema(enumeration = {"PICK", "COUNT", "HOLD"})) @QueryParam("type") String type,
            @QueryParam("limit") String limit, @QueryParam("offset") String offset) {
        access.check();
        if (siteId != null) {
            Validation.site(siteId);
        }
        if (type != null && !TYPES.contains(type)) {
            throw Validation.badRequest("type must be PICK, COUNT or HOLD");
        }
        int size = Validation.bounded(limit, "limit", DEFAULT_LIMIT, 1, MAX_LIMIT);
        int start = Validation.bounded(offset, "offset", 0, 0, Integer.MAX_VALUE);
        return store.siteDecisions(siteId, type, size, start).stream().map(Views.SiteDecision::of).toList();
    }

    private JsonNode detail(String json) {
        if (json == null) {
            return null;
        }
        try {
            return mapper.readTree(json);
        } catch (JsonProcessingException error) {
            throw new IllegalStateException(error);
        }
    }
}
