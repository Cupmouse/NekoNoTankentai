package net.nekonium.explorer.util;

public class UnconfiguredPropertyException extends Exception {

    public UnconfiguredPropertyException() {
    }

    public UnconfiguredPropertyException(String message) {
        super(message);
    }

    public UnconfiguredPropertyException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnconfiguredPropertyException(Throwable cause) {
        super(cause);
    }

    public UnconfiguredPropertyException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
