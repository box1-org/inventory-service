package org.box1.inventory.domain;

/** An event that can never be processed. It goes to the dead-letter topic without retries. */
public class PoisonEventException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public PoisonEventException(String reason) {
        super(reason);
    }
}
