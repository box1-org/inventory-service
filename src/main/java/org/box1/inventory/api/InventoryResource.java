package org.box1.inventory.api;

import java.util.List;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import org.box1.inventory.domain.RequestProblem;
import org.box1.inventory.store.InventoryStore;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;

/** The read API from box1-ops-contracts openapi/inventory-service.yaml. There are no mutation routes here. */
@Path("/api/v1")
@Produces(MediaType.APPLICATION_JSON)
public class InventoryResource {
    private static final int DEFAULT_LIMIT = 50;
    private static final int MAX_LIMIT = 100;

    private final InventoryStore store;
    private final ReadAccess access;

    public InventoryResource(InventoryStore store, ReadAccess access) {
        this.store = store;
        this.access = access;
    }

    @GET
    @Path("/inventory")
    @Operation(operationId = "listInventory", summary = "Available-to-promise by site and SKU")
    @APIResponse(responseCode = "200", description = "Stock levels ordered by siteId, then sku",
            content = @Content(mediaType = MediaType.APPLICATION_JSON,
                    schema = @Schema(type = SchemaType.ARRAY, implementation = Views.Stock.class)))
    @APIResponse(responseCode = "400", description = "Invalid filter or page", content = @Content(mediaType = Problem.MEDIA_TYPE,
            schema = @Schema(implementation = Problem.class)))
    @APIResponse(responseCode = "401", description = "A token is required by the read policy", content = @Content(mediaType = Problem.MEDIA_TYPE,
            schema = @Schema(implementation = Problem.class)))
    public List<Views.Stock> list(@QueryParam("siteId") String siteId, @QueryParam("sku") String sku,
            @QueryParam("limit") String limit, @QueryParam("offset") String offset) {
        access.check();
        if (siteId != null) {
            Validation.site(siteId);
        }
        if (sku != null) {
            Validation.sku(sku);
        }
        int size = Validation.bounded(limit, "limit", DEFAULT_LIMIT, 1, MAX_LIMIT);
        int start = Validation.bounded(offset, "offset", 0, 0, Integer.MAX_VALUE);
        return store.stock(siteId, sku, size, start).stream().map(Views.Stock::of).toList();
    }

    @GET
    @Path("/reservations/{reservationId}")
    @Operation(operationId = "getReservation", summary = "Read one reservation")
    @APIResponse(responseCode = "200", description = "Reservation",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = Views.Hold.class)))
    @APIResponse(responseCode = "400", description = "Invalid reservation ID", content = @Content(mediaType = Problem.MEDIA_TYPE,
            schema = @Schema(implementation = Problem.class)))
    @APIResponse(responseCode = "401", description = "A token is required by the read policy", content = @Content(mediaType = Problem.MEDIA_TYPE,
            schema = @Schema(implementation = Problem.class)))
    @APIResponse(responseCode = "404", description = "Reservation not found", content = @Content(mediaType = Problem.MEDIA_TYPE,
            schema = @Schema(implementation = Problem.class)))
    public Views.Hold reservation(@PathParam("reservationId") String reservationId) {
        access.check();
        Validation.reservation(reservationId);
        return store.reservation(reservationId).map(Views.Hold::of)
                .orElseThrow(() -> new RequestProblem(404, "Not Found", "Reservation not found"));
    }
}
