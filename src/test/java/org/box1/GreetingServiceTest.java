package org.box1;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GreetingServiceTest {
    private final GreetingService service = new GreetingService("Hello");

    @Test
    void greetsAndTrimsTheName() {
        assertEquals("Hello, Ada!", service.greet("  Ada  "));
    }

    @Test
    void rejectsMissingOrExcessivelyLongNames() {
        assertThrows(IllegalArgumentException.class, () -> service.greet(null));
        assertThrows(IllegalArgumentException.class, () -> service.greet("  "));
        assertThrows(IllegalArgumentException.class, () -> service.greet("a".repeat(81)));
        assertEquals("Hello, " + "a".repeat(80) + "!", service.greet("a".repeat(80)));
    }

    @Test
    void usesTheConfiguredPrefix() {
        assertEquals("Welcome, Ada!", new GreetingService("Welcome").greet("Ada"));
    }
}
