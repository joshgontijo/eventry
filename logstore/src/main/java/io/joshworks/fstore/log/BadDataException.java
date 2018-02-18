package io.joshworks.fstore.log;

public class BadDataException extends RuntimeException {

    public BadDataException(String message) {
        super(message);
    }

    public BadDataException(String message, Throwable cause) {
        super(message, cause);
    }
}
