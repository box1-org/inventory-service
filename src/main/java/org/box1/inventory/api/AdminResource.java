package org.box1.inventory.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import jakarta.annotation.security.RolesAllowed;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.box1.inventory.ReservationService;
import org.eclipse.microprofile.jwt.JsonWebToken;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.security.SecurityRequirement;
import org.eclipse.microprofile.openapi.annotations.security.SecurityScheme;
import org.eclipse.microprofile.openapi.annotations.enums.SecuritySchemeType;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;

/**
 * Operator routes. Every call needs a verified bearer token with the inventory-admin role, and is recorded with the
 * token's subject. The annotation is the guard, because the public Route exposes every path.
 */
@Path("/internal/v1")
@Produces(MediaType.APPLICATION_JSON)
@RolesAllowed(AdminResource.ROLE)
@SecurityScheme(securitySchemeName = "bearer", type = SecuritySchemeType.HTTP, scheme = "bearer", bearerFormat = "JWT")
@SecurityRequirement(name = "bearer", scopes = AdminResource.ROLE)
public class AdminResource {
    static final String ROLE = "inventory-admin";
    private static final Logger LOG = Logger.getLogger(AdminResource.class);
    private static final int MAX_RESTOCK = 1_000_000;

    private final ReservationService reservations;
    private final JsonWebToken token;

    public AdminResource(ReservationService reservations, JsonWebToken token) {
        this.reservations = reservations;
        this.token = token;
    }

    @POST
    @Path("/stock/{siteId}/{sku}/restock")
    @Consumes(MediaType.APPLICATION_JSON)
    @Operation(operationId = "restock", summary = "Add stock. Authenticated, idempotent and audited.")
    @RequestBody(required = true, content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = Views.Restock.class)))
    @APIResponse(responseCode = "200", description = "Stock after the restock. A repeat of the same key and body changes nothing.",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = Views.Stock.class)))
    @APIResponse(responseCode = "400", description = "Invalid request", content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
    @APIResponse(responseCode = "401", description = "No valid token", content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
    @APIResponse(responseCode = "403", description = "Token lacks the role", content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
    @APIResponse(responseCode = "404", description = "No such stock record", content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
    @APIResponse(responseCode = "409", description = "Key used with a different request", content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
    public Views.Stock restock(@PathParam("siteId") String siteId, @PathParam("sku") String sku,
            @HeaderParam("Idempotency-Key") String key, Views.Restock body) {
        Validation.site(siteId);
        Validation.sku(sku);
        Validation.key(key);
        if (body == null || body.quantity() == null || body.quantity() < 1 || body.quantity() > MAX_RESTOCK) {
            throw Validation.badRequest("quantity must be 1 to 1000000");
        }
        Validation.reason(body.reason());
        Views.Stock level = Views.Stock.of(reservations.restock(siteId, sku, body.quantity(), body.reason(), key, token.getSubject()));
        LOG.infov("Restock applied: siteId={0}", siteId);
        return level;
    }

    @POST
    @Path("/reservations/{reservationId}/release")
    @Consumes(MediaType.APPLICATION_JSON)
    @Operation(operationId = "releaseReservation", summary = "Return a hold to available. Never applies to a consumed reservation.")
    @RequestBody(required = true, content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = Views.Release.class)))
    @APIResponse(responseCode = "200", description = "Reservation after release. Repeating a release changes nothing.",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = Views.Hold.class)))
    @APIResponse(responseCode = "400", description = "Invalid request", content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
    @APIResponse(responseCode = "401", description = "No valid token", content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
    @APIResponse(responseCode = "403", description = "Token lacks the role", content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
    @APIResponse(responseCode = "404", description = "No such reservation", content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
    @APIResponse(responseCode = "409", description = "Reservation is consumed or expired", content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
    public Views.Hold release(@PathParam("reservationId") String reservationId, Views.Release body) {
        Validation.reservation(reservationId);
        if (body == null) {
            throw Validation.badRequest("reason is required and must be 1 to 200 characters");
        }
        Validation.reason(body.reason());
        Views.Hold hold = Views.Hold.of(reservations.release(reservationId, token.getSubject(), body.reason()));
        MDC.put("orderId", hold.orderId());
        try {
            LOG.infov("Reservation release requested: reservationId={0}", reservationId);
        } finally {
            MDC.remove("orderId");
        }
        return hold;
    }

    /** Applies to this resource only: unreadable bodies and media types get Problem responses, without parser text. */
    @ServerExceptionMapper
    Response framework(WebApplicationException error) {
        int status = error.getResponse().getStatus();
        String title = error.getResponse().getStatusInfo().getReasonPhrase();
        String detail = status == 400 ? "Request body is not valid" : title;
        return Response.status(status).type(Problem.MEDIA_TYPE).entity(Problem.of(status, title, detail)).build();
    }

    /** Takes precedence over the framework's own JSON error body for binding failures. */
    @ServerExceptionMapper
    Response unreadable(JsonProcessingException error) {
        return Response.status(400).type(Problem.MEDIA_TYPE).entity(Problem.of(400, "Bad Request", "Request body is not valid")).build();
    }

    /** The framework maps this subtype itself unless a mapper names it exactly. */
    @ServerExceptionMapper
    Response unreadable(MismatchedInputException error) {
        return unreadable((JsonProcessingException) error);
    }
}
