package io.joshworks.fstore.es;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.LongSerializer;
import io.joshworks.fstore.serializer.VStringSerializer;

import java.nio.ByteBuffer;

public class EventSerializer implements Serializer<Event> {

    private final Serializer<String> strSerializer = new VStringSerializer();
    private final Serializer<Long> longSerializer = new LongSerializer();

    @Override
    public ByteBuffer toBytes(Event data) {
        int dataLength = data.data == null ? 0 : data.data.length();
        int typeLength = data.type == null ? 0 : data.type.length();

        ByteBuffer bb = ByteBuffer.allocate(dataLength + typeLength + Long.BYTES);

        return (ByteBuffer) bb.flip();
    }

    @Override
    public void writeTo(Event data, ByteBuffer dest) {
        strSerializer.writeTo(data.type, dest);
        strSerializer.writeTo(data.data, dest);
        longSerializer.writeTo(data.timestamp, dest);
    }


    @Override
    public Event fromBytes(ByteBuffer buffer) {
        String type = strSerializer.fromBytes(buffer);
        String data = strSerializer.fromBytes(buffer);
        long timestamp = longSerializer.fromBytes(buffer);

        return new Event(type, data, timestamp);
    }

}
