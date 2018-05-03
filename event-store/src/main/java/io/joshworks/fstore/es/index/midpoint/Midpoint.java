package io.joshworks.fstore.es.index.midpoint;


import io.joshworks.fstore.es.index.IndexEntry;

import java.util.Objects;

public class Midpoint implements Comparable<IndexEntry>{

    public static final int BYTES = IndexEntry.BYTES + Long.BYTES;

    public final IndexEntry key;
    public final long position;

    public Midpoint(IndexEntry key, long position) {
        this.key = key;
        this.position = position;
    }

    @Override
    public int compareTo(IndexEntry o) {
        return key.compareTo(o);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Midpoint midpoint = (Midpoint) o;
        return position == midpoint.position &&
                Objects.equals(key, midpoint.key);
    }

    @Override
    public int hashCode() {

        return Objects.hash(key, position);
    }
}
