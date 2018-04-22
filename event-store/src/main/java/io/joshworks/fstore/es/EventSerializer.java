package io.joshworks.fstore.es;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.VStringSerializer;

import java.nio.ByteBuffer;

public class EventSerializer implements Serializer<Event> {

    private final Serializer<String> strSerializer = new VStringSerializer();

    @Override
    public ByteBuffer toBytes(Event data) {
        int dataLength = VStringSerializer.sizeOf(data.data);
        int typeLength = VStringSerializer.sizeOf(data.type);

        ByteBuffer bb = ByteBuffer.allocate(dataLength + typeLength + Long.BYTES);
        writeTo(data, bb);

        return (ByteBuffer) bb.flip();
    }

    @Override
    public void writeTo(Event data, ByteBuffer dest) {
        strSerializer.writeTo(data.type, dest);
        strSerializer.writeTo(data.data, dest);
        dest.putLong(data.timestamp);
    }


    @Override
    public Event fromBytes(ByteBuffer buffer) {
        String type = strSerializer.fromBytes(buffer);
        String data = strSerializer.fromBytes(buffer);
        long timestamp = buffer.getLong();

        return new Event(type, data, timestamp);
    }

}
