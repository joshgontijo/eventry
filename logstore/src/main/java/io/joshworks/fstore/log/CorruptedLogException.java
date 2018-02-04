package io.joshworks.fstore.log;

public class CorruptedLogException extends RuntimeException {

    public CorruptedLogException(String message) {
        super(message);
    }

    public CorruptedLogException(String message, Throwable cause) {
        super(message, cause);
    }
}
