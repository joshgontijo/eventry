package io.joshworks.fstore.es.projections;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.es.index.IndexEntry;
import io.joshworks.fstore.es.index.disk.IndexEntrySerializer;

import java.nio.ByteBuffer;

public class ProjectionEntrySerializer implements Serializer<ProjectionEntry> {

    private final IndexEntrySerializer indexEntrySerializer = new IndexEntrySerializer();

    @Override
    public ByteBuffer toBytes(ProjectionEntry data) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES + Long.BYTES +  IndexEntry.BYTES);
        writeTo(data, byteBuffer);
        return byteBuffer.flip();
    }

    @Override
    public void writeTo(ProjectionEntry data, ByteBuffer dest) {
        dest.putInt(data.type);
        dest.putLong(data.timestamp);
        indexEntrySerializer.writeTo(data.indexEntry, dest);
    }

    @Override
    public ProjectionEntry fromBytes(ByteBuffer buffer) {
        int type = buffer.getInt();
        long timestamp = buffer.getLong();
        IndexEntry entry = indexEntrySerializer.fromBytes(buffer);
        return new ProjectionEntry(type, timestamp, entry);

    }
}