package io.joshworks.fstore.index;

public class Range<K extends Comparable<K>>{

    static final int NO_LIMIT = -1;
    static final int SKIP_NONE = 0;

    K startInclusive;
    K endExclusive;
    long skip = SKIP_NONE;
    long limit = NO_LIMIT;

    public Range() {
    }

    public static <K extends Comparable<K>> Range<K> of(Class<K> type) {
        return new Range<>();
    }

    public Range<K> startInclusive(final K startInclusive) {
        this.startInclusive = startInclusive;
        return this;
    }

    public Range<K> endExclusive(final K endExclusive) {
        this.endExclusive = endExclusive;
        return this;
    }

    public Range<K> skip(final long skip) {
        this.skip = skip;
        return this;
    }

    public Range<K> limit(final long limit) {
        this.limit = limit;
        return this;
    }

    public K startInclusive() {
        return startInclusive;
    }

    public K endExclusive() {
        return endExclusive;
    }

    public long skip() {
        return skip;
    }

    public long limit() {
        return limit;
    }
}
