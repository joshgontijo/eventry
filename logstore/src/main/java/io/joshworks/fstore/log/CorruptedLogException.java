package io.joshworks.fstore.log;

class CorruptedLogException extends RuntimeException {

    CorruptedLogException(String message) {
        super(message);
    }

    CorruptedLogException(String message, Throwable cause) {
        super(message, cause);
    }
}
