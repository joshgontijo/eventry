package io.joshworks.fstore.log;

public class ChecksumException extends RuntimeException {

    public ChecksumException() {
        super("Checksum verification failed");
    }
}
