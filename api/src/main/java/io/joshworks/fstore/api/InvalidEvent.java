package io.joshworks.fstore.api;

/**
 * Created by Josh Gontijo on 4/14/17.
 */
public class InvalidEvent extends RuntimeException {

    public InvalidEvent(String message) {
        super(message);
    }

    public InvalidEvent(String message, Throwable cause) {
        super(message, cause);
    }
}
