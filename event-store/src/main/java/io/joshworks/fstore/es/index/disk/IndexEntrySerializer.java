package io.joshworks.fstore.es.index.disk;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.es.index.IndexEntry;

import java.nio.ByteBuffer;

public class IndexEntrySerializer implements Serializer<IndexEntry> {

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
