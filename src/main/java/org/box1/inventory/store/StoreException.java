package org.box1.inventory.store;

import java.sql.SQLException;

/** A database failure. Carries only the SQL state, so no row values reach logs or dead-letter headers. */
public class StoreException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final String sqlState;

    public StoreException(SQLException cause) {
        super("Database operation failed: SQLState " + cause.getSQLState());
        this.sqlState = cause.getSQLState();
    }

    public String sqlState() {
        return sqlState;
    }

    /** The database cannot serve at all. Waiting fixes this, and no event is at fault. */
    public boolean isUnavailable() {
        return sqlState == null || sqlState.startsWith("08") || sqlState.startsWith("53") || sqlState.startsWith("57P");
    }

    /** The data can never be stored: a bad value or a constraint it will always break. Retrying cannot help. */
    public boolean isPermanent() {
        return sqlState != null && (sqlState.startsWith("22") || sqlState.startsWith("23") && !"23505".equals(sqlState));
    }

    /** Deadlock, serialization failure, or a concurrent writer winning a unique key. Retrying resolves each one. */
    public boolean isConflict() {
        return "40001".equals(sqlState) || "40P01".equals(sqlState) || "23505".equals(sqlState);
    }
}
