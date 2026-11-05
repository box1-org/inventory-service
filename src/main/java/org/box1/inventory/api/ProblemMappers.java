package org.box1.inventory.api;

import io.quarkus.security.AuthenticationFailedException;
import io.quarkus.security.ForbiddenException;
import io.quarkus.security.UnauthorizedException;
import jakarta.ws.rs.core.Response;
import org.box1.inventory.domain.RequestProblem;
import org.box1.inventory.store.StoreException;
import org.jboss.logging.Logger;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;

/** Maps failures to RFC 9457 responses. Parser, token and driver messages are not returned to callers. */
public class ProblemMappers {
    private static final Logger LOG = Logger.getLogger(ProblemMappers.class);

    @ServerExceptionMapper
    public Response problem(RequestProblem error) {
        return response(Problem.of(error.status(), error.title(), error.getMessage()));
    }

    @ServerExceptionMapper
    public Response store(StoreException error) {
        LOG.error(error.getMessage());
        return response(Problem.of(503, "Service Unavailable", "The inventory store is unavailable"));
    }

    @ServerExceptionMapper
    public Response unauthorized(UnauthorizedException error) {
        return unauthenticated();
    }

    @ServerExceptionMapper
    public Response authenticationFailed(AuthenticationFailedException error) {
        return unauthenticated();
    }

    @ServerExceptionMapper
    public Response forbidden(ForbiddenException error) {
        return response(Problem.of(403, "Forbidden", "This operation needs the inventory-admin role"));
    }

    private static Response unauthenticated() {
        return Response.fromResponse(response(Problem.of(401, "Unauthorized", "A valid bearer token is required")))
                .header("WWW-Authenticate", "Bearer").build();
    }

    private static Response response(Problem problem) {
        return Response.status(problem.status()).type(Problem.MEDIA_TYPE).entity(problem).build();
    }
}
