package net.nekonium.explorer.util;

public class IllegalBlockchainStateException extends RuntimeException {

    public IllegalBlockchainStateException() {
    }

    public IllegalBlockchainStateException(String message) {
        super(message);
    }

    public IllegalBlockchainStateException(String message, Throwable cause) {
        super(message, cause);
    }

    public IllegalBlockchainStateException(Throwable cause) {
        super(cause);
    }

    public IllegalBlockchainStateException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
