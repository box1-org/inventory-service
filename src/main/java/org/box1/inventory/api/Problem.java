package org.box1.inventory.api;

import com.fasterxml.jackson.annotation.JsonInclude;

/** RFC 9457 problem body. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record Problem(String type, String title, int status, String detail) {
    public static final String MEDIA_TYPE = "application/problem+json";

    static Problem of(int status, String title, String detail) {
        return new Problem("about:blank", title, status, detail);
    }
}
