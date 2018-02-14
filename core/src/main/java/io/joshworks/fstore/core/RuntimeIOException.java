package io.joshworks.fstore.core;

import java.io.IOException;

public class RuntimeIOException extends RuntimeException {

    public RuntimeIOException(String message) {
        super(message);
    }

    public RuntimeIOException(String message, Throwable cause) {
        super(message, cause);
    }

    public static RuntimeException of(IOException ioex) {
        return new RuntimeIOException(ioex.getMessage());
    }
}
