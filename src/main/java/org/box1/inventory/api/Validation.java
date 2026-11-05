package org.box1.inventory.api;

import java.util.regex.Pattern;

import org.box1.inventory.domain.RequestProblem;

/** Request field rules shared by the read and admin routes. Failure details never echo the value. */
final class Validation {
    private static final Pattern SITE = Pattern.compile("^site_[A-Za-z0-9_-]{1,120}$");
    private static final Pattern SKU = Pattern.compile("^[^\\p{Cntrl}]{1,80}$");
    private static final Pattern RESERVATION = Pattern.compile("^res_[A-Za-z0-9_-]{1,124}$");
    private static final int MAX_REASON = 200;
    private static final int MAX_KEY = 255;

    private Validation() {}

    static RequestProblem badRequest(String detail) {
        return new RequestProblem(400, "Bad Request", detail);
    }

    static void site(String value) {
        if (value == null || !SITE.matcher(value).matches()) {
            throw badRequest("siteId is invalid");
        }
    }

    static void sku(String value) {
        if (value == null || !SKU.matcher(value).matches()) {
            throw badRequest("sku is invalid");
        }
    }

    static void reservation(String value) {
        if (value == null || !RESERVATION.matcher(value).matches()) {
            throw badRequest("reservationId is invalid");
        }
    }

    static void reason(String value) {
        if (value == null || value.isBlank() || value.length() > MAX_REASON || value.chars().anyMatch(Character::isISOControl)) {
            throw badRequest("reason is required and must be 1 to 200 characters");
        }
    }

    static void key(String value) {
        if (value == null || value.isEmpty() || value.length() > MAX_KEY || value.chars().anyMatch(Character::isISOControl)) {
            throw badRequest("Idempotency-Key is required and must be 1 to 255 characters");
        }
    }

    static int bounded(String value, String name, int fallback, int min, int max) {
        if (value == null) {
            return fallback;
        }
        try {
            int parsed = Integer.parseInt(value);
            if (parsed < min || parsed > max) {
                throw badRequest(name + " is out of range");
            }
            return parsed;
        } catch (NumberFormatException error) {
            throw badRequest(name + " must be an integer");
        }
    }
}
