package net.nekonium.explorer.util;

public class IllegalDatabaseStateException extends RuntimeException {

    public IllegalDatabaseStateException() {
    }

    public IllegalDatabaseStateException(String message) {
        super(message);
    }

    public IllegalDatabaseStateException(String message, Throwable cause) {
        super(message, cause);
    }

    public IllegalDatabaseStateException(Throwable cause) {
        super(cause);
    }

    public IllegalDatabaseStateException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
