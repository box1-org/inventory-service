package org.box1;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.OpenAPIDefinition;
import org.eclipse.microprofile.openapi.annotations.info.Info;

@Path("/api/greetings")
@Produces(MediaType.APPLICATION_JSON)
@OpenAPIDefinition(info = @Info(title = "inventory-service REST API", version = "1.0.0"))
public class GreetingResource {
    private final GreetingService greetings;
    private final Counter counter;

    public GreetingResource(GreetingService greetings, MeterRegistry registry) {
        this.greetings = greetings;
        this.counter = registry.counter("box1.greetings");
    }

    @GET
    @Operation(summary = "Return a configurable greeting")
    public Greeting greet(@QueryParam("name") @DefaultValue("developer") String name) {
        try {
            String message = greetings.greet(name);
            counter.increment();
            return new Greeting(message, "inventory-service");
        } catch (IllegalArgumentException error) {
            throw new BadRequestException(error.getMessage());
        }
    }

    public record Greeting(String message, String service) {}
}
