package io.joshworks.fstore.es.log;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.VStringSerializer;

import java.nio.ByteBuffer;

public class EventSerializer implements Serializer<Event> {

    private final Serializer<String> strSerializer = new VStringSerializer();

    @Override
    public ByteBuffer toBytes(Event data) {
        int typeLength = VStringSerializer.sizeOf(data.type());
        int streamNameLength = VStringSerializer.sizeOf(data.stream());

        ByteBuffer bb = ByteBuffer.allocate(data.data().length + typeLength + streamNameLength + Long.BYTES);
        writeTo(data, bb);

        return bb.flip();
    }

    @Override
    public void writeTo(Event data, ByteBuffer dest) {
        strSerializer.writeTo(data.type(), dest);
        strSerializer.writeTo(data.stream(), dest);
        dest.putLong(data.timestamp());
        dest.put(data.data());
    }


    @Override
    public Event fromBytes(ByteBuffer buffer) {
        String type = strSerializer.fromBytes(buffer);
        String stream = strSerializer.fromBytes(buffer);
        long timestamp = buffer.getLong();
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);

        return Event.of(stream, type, data, timestamp);
    }

}
