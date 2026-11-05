package org.box1.inventory.api;

import java.util.Set;
import java.util.stream.Collectors;

import io.quarkus.security.identity.AuthenticationRequestContext;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.identity.SecurityIdentityAugmentor;
import io.quarkus.security.runtime.QuarkusSecurityIdentity;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import jakarta.json.JsonString;
import org.eclipse.microprofile.jwt.JsonWebToken;

/**
 * Roles come only from this client's own claim. When that claim is absent, the JWT layer otherwise falls back to a
 * top-level groups claim, which another Keycloak mapper could populate.
 */
@ApplicationScoped
public class ClientRoleAugmentor implements SecurityIdentityAugmentor {
    private static final String CLIENT = "inventory-service";

    @Override
    public Uni<SecurityIdentity> augment(SecurityIdentity identity, AuthenticationRequestContext context) {
        if (identity.isAnonymous() || !(identity.getPrincipal() instanceof JsonWebToken token)) {
            return Uni.createFrom().item(identity);
        }
        QuarkusSecurityIdentity.Builder builder = QuarkusSecurityIdentity.builder().setPrincipal(token)
                .addCredentials(identity.getCredentials()).addAttributes(identity.getAttributes())
                .addRoles(roles(token));
        return Uni.createFrom().item(builder.build());
    }

    private static Set<String> roles(JsonWebToken token) {
        JsonObject access = token.getClaim("resource_access");
        JsonObject client = access == null ? null : access.getJsonObject(CLIENT);
        JsonArray roles = client == null ? null : client.getJsonArray("roles");
        if (roles == null) {
            return Set.of();
        }
        return roles.getValuesAs(JsonString.class).stream().map(JsonString::getString).collect(Collectors.toSet());
    }
}
