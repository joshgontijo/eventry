package io.joshworks.fstore.event;

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
