package io.joshworks.fstore;

public class Position {
    public final long start;
    public final int length;

    private Position(long start, int length) {
        this.start = start;
        this.length = length;
    }

    public static Position of(long start, int length) {
        return new Position(start, length);
    }


}
