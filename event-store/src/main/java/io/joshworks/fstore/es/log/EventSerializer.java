package io.joshworks.fstore.es.log;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.VStringSerializer;

import java.nio.ByteBuffer;

public class EventSerializer implements Serializer<EventRecord> {

    private final Serializer<String> strSerializer = new VStringSerializer();

    @Override
    public ByteBuffer toBytes(EventRecord data) {
        int typeLength = VStringSerializer.sizeOf(data.type);
        int streamNameLength = VStringSerializer.sizeOf(data.stream);

        ByteBuffer bb = ByteBuffer.allocate(typeLength + streamNameLength + Integer.BYTES + Long.BYTES + Integer.BYTES + data.data.length + data.metadata.length);
        writeTo(data, bb);

        return bb.flip();
    }

    @Override
    public void writeTo(EventRecord data, ByteBuffer dest) {
        strSerializer.writeTo(data.type, dest);
        strSerializer.writeTo(data.stream, dest);
        dest.putInt(data.version);
        dest.putLong(data.timestamp);

        dest.putInt(data.data.length);
        dest.put(data.data);
        dest.put(data.metadata);
    }


    @Override
    public EventRecord fromBytes(ByteBuffer buffer) {
        String type = strSerializer.fromBytes(buffer);
        String stream = strSerializer.fromBytes(buffer);
        int version = buffer.getInt();
        long timestamp = buffer.getLong();

        int dataLength = buffer.getInt();
        byte[] data = new byte[dataLength];
        buffer.get(data);

        byte[] metadata = new byte[buffer.remaining()];
        buffer.get(metadata);

        return new EventRecord(stream, type, version, timestamp, data, metadata);
    }

}
