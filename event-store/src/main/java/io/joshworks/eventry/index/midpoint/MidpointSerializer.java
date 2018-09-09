package io.joshworks.eventry.index.midpoint;

import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.eventry.index.disk.IndexEntrySerializer;
import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class MidpointSerializer implements Serializer<Midpoint> {

    private final Serializer<IndexEntry> indexEntrySerializer = new IndexEntrySerializer();

    @Override
    public ByteBuffer toBytes(Midpoint data) {
        ByteBuffer bb = ByteBuffer.allocate(Midpoint.BYTES);
        writeTo(data, bb);

        return (ByteBuffer) bb.flip();
    }

    @Override
    public void writeTo(Midpoint data, ByteBuffer dest) {
        indexEntrySerializer.writeTo(data.key, dest);
        dest.putLong(data.position);
    }

    @Override
    public Midpoint fromBytes(ByteBuffer buffer) {
        IndexEntry indexEntry = indexEntrySerializer.fromBytes(buffer);
        long position = buffer.getLong();
        return new Midpoint(indexEntry, position);
    }
}
