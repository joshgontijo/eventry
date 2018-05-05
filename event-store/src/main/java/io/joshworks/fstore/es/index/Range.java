package io.joshworks.fstore.es.index;

public class Range {

    public final long stream;
    public final int startVersionInclusive;
    public final int endVersionExclusive;

    private Range(long stream, int startVersionInclusive, int endVersionExclusive) {
        if(startVersionInclusive <= 0) {
            throw new IllegalArgumentException("Version range must be greater than zero");
        }
        this.stream = stream;
        this.startVersionInclusive = startVersionInclusive;
        this.endVersionExclusive = endVersionExclusive;
    }

    public static Range of(long stream, int start) {
        return new Range(stream, start, Integer.MAX_VALUE);
    }

    public static Range of(long stream, int start, int end) {
        return new Range(stream, start, end);
    }

    public static Range allOf(long stream) {
        return new Range(stream, 1, Integer.MAX_VALUE);
    }

    public IndexEntry start() {
        return IndexEntry.of(stream, startVersionInclusive, 0);
    }

    public IndexEntry end() {
        return IndexEntry.of(stream, endVersionExclusive, 0);
    }


}
