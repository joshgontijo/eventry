package io.joshworks.fstore.log;

public class ChecksumException extends RuntimeException {

    public ChecksumException() {
        super("Checksum verification failed");
    }

    public ChecksumException(long position) {
        super("Checksum verification failed at position " + position);
    }
}
