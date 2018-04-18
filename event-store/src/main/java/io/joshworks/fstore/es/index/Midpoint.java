package io.joshworks.fstore.es.index;


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
        return o.compareTo(key);
    }
}
