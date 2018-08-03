package io.joshworks.fstore.es.index;

public class Range {

    public final long stream;
    public final int startVersionInclusive;
    public final int endVersionExclusive;

    public static final int START_VERSION = 0;

    private Range(long stream, int startVersionInclusive, int endVersionExclusive) {
        if(startVersionInclusive < START_VERSION) {
            throw new IllegalArgumentException("Version range must be greater or equals zero");
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
        return new Range(stream, START_VERSION, Integer.MAX_VALUE);
    }

    public IndexEntry start() {
        return IndexEntry.of(stream, startVersionInclusive, START_VERSION);
    }

    public IndexEntry end() {
        return IndexEntry.of(stream, endVersionExclusive, START_VERSION);
    }


}
