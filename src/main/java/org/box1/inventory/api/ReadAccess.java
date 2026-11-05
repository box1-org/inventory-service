package org.box1.inventory.api;

import io.quarkus.security.UnauthorizedException;
import io.quarkus.security.identity.SecurityIdentity;
import jakarta.enterprise.context.RequestScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/** The read policy. Anything other than an exact "permit" requires a token, so a mistyped value fails closed. */
@RequestScoped
public class ReadAccess {
    private final SecurityIdentity identity;
    private final String policy;

    public ReadAccess(SecurityIdentity identity, @ConfigProperty(name = "inventory.security.read-policy") String policy) {
        this.identity = identity;
        this.policy = policy;
    }

    void check() {
        if (!"permit".equals(policy) && identity.isAnonymous()) {
            throw new UnauthorizedException();
        }
    }
}
