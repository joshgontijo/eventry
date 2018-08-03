package io.joshworks.fstore.es.index;

import java.util.Objects;

public class IndexEntry implements Comparable<IndexEntry> {

    public static final int BYTES = Long.BYTES + Integer.BYTES + Long.BYTES;

    public final long stream;
    public final int version;
    public final long position;

    public static final int NO_VERSION = -1;

    private IndexEntry(long stream, int version, long position) {
        if(version <= NO_VERSION) {
            throw new IllegalArgumentException("Version must be at least zero");
        }
        if(position < 0) {
            throw new IllegalArgumentException("Position must be positive");
        }
        this.stream = stream;
        this.version = version;
        this.position = position;
    }

    public static IndexEntry of(long stream, int version, long position) {
        return new IndexEntry(stream, version, position);
    }


    @Override
    public int compareTo(IndexEntry other) {
        int keyCmp = Long.compare(stream, other.stream);
        if (keyCmp == 0) {
            keyCmp = version - other.version;
            if (keyCmp != 0)
                return keyCmp;
        }
        return keyCmp;
    }

    public boolean greaterThan(IndexEntry other) {
        return this.compareTo(other) > 0;
    }

    public boolean lessThan(IndexEntry other) {
        return this.compareTo(other) < 0;
    }

    public boolean greatOrEqualsTo(IndexEntry other) {
        return this.compareTo(other) >= 0;
    }


    @Override
    public String toString() {
        return "IndexEntry{" + "stream=" + stream +
                ", version=" + version +
                ", position=" + position +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexEntry that = (IndexEntry) o;
        return stream == that.stream &&
                version == that.version;
    }

    @Override
    public int hashCode() {
        return Objects.hash(stream, version);
    }
}
