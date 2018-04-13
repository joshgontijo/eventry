package io.joshworks.fstore.es;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;
import java.util.Objects;

public class IndexKey implements Comparable<IndexKey> {

    public final int stream;
    public final int version;
    public final long position;

    public IndexKey(int stream, int version, long position) {
        this.stream = stream;
        this.version = version;
        this.position = position;
    }


    @Override
    public int compareTo(IndexKey other) {
        int keyCmp = stream - other.stream;
        if (keyCmp == 0)
        {
            keyCmp = version - other.version;
            if (keyCmp != 0)
                return keyCmp;
        }
        if (keyCmp != 0)
            return keyCmp;
        return (int) (position - other.position);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexKey indexKey = (IndexKey) o;
        return stream == indexKey.stream &&
                version == indexKey.version &&
                position == indexKey.position;
    }

    @Override
    public int hashCode() {
        return Objects.hash(stream, version, position);
    }

    public static Serializer<IndexKey> serializer() {
        return new Serializer<IndexKey>() {
            @Override
            public ByteBuffer toBytes(IndexKey data) {
                ByteBuffer bb = ByteBuffer.allocate(Integer.BYTES + Integer.BYTES + Long.BYTES);//16
                return bb.putInt(data.stream).putInt(data.version).putLong(data.position);
            }

            @Override
            public IndexKey fromBytes(ByteBuffer buffer) {
                return new IndexKey(buffer.getInt(), buffer.getInt(), buffer.getLong());
            }
        };
    }

}
