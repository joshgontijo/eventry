package io.joshworks.fstore.log.segment;

public class CorruptedSegmentException extends RuntimeException {
    public CorruptedSegmentException(String message) {
        super(message);
    }
}
