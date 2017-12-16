package net.nekonium.explorer;

public class AddressPoolException extends RuntimeException {

    public AddressPoolException() {
    }

    public AddressPoolException(String message) {
        super(message);
    }

    public AddressPoolException(String message, Throwable cause) {
        super(message, cause);
    }

    public AddressPoolException(Throwable cause) {
        super(cause);
    }

    public AddressPoolException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
