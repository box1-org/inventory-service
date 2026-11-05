package org.box1;

import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class GreetingService {
    private final String prefix;

    public GreetingService(@ConfigProperty(name = "greeting.prefix") String prefix) {
        this.prefix = prefix;
    }

    public String greet(String name) {
        if (name == null || name.isBlank() || name.strip().length() > 80) {
            throw new IllegalArgumentException("name must contain between 1 and 80 characters");
        }
        return prefix + ", " + name.strip() + "!";
    }
}
