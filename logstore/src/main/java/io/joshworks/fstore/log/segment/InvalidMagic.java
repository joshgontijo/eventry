package io.joshworks.fstore.log.segment;

public class InvalidMagic extends SegmentException {

    public InvalidMagic(String expected, String actual) {
        super("Invalid magic: Expected: '" + expected + "', actual: '" + actual + "'");
    }
}
