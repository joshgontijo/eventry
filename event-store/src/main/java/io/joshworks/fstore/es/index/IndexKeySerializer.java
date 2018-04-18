package io.joshworks.fstore.es.index;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class IndexKeySerializer implements Serializer<IndexEntry> {

    @Override
    public ByteBuffer toBytes(IndexEntry data) {
        ByteBuffer bb = ByteBuffer.allocate(Long.BYTES + Integer.BYTES + Long.BYTES);//20
        writeTo(data, bb);
        return (ByteBuffer) bb.flip();
    }

    @Override
    public void writeTo(IndexEntry data, ByteBuffer dest) {
        dest.putLong(data.stream).putInt(data.version).putLong(data.position);
    }

    @Override
    public IndexEntry fromBytes(ByteBuffer buffer) {
        return IndexEntry.of(buffer.getLong(), buffer.getInt(), buffer.getLong());
    }
}
