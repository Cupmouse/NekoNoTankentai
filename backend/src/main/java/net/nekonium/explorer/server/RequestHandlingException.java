package net.nekonium.explorer.server;

public class RequestHandlingException extends Exception {

    public RequestHandlingException() {
    }

    public RequestHandlingException(String message) {
        super(message);
    }

    public RequestHandlingException(String message, Throwable cause) {
        super(message, cause);
    }

    public RequestHandlingException(Throwable cause) {
        super(cause);
    }

    public RequestHandlingException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
