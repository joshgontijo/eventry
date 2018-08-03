package io.joshworks.fstore.es.log;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.VStringSerializer;

import java.nio.ByteBuffer;

public class EventSerializer implements Serializer<Event> {

    private final Serializer<String> strSerializer = new VStringSerializer();

    @Override
    public ByteBuffer toBytes(Event data) {
        int typeLength = VStringSerializer.sizeOf(data.type());

        ByteBuffer bb = ByteBuffer.allocate(data.data().length + typeLength + (Long.BYTES * 2));
        writeTo(data, bb);

        return (ByteBuffer) bb.flip();
    }

    @Override
    public void writeTo(Event data, ByteBuffer dest) {
        strSerializer.writeTo(data.type(), dest);
        dest.putLong(data.timestamp());
        dest.putLong(data.sequence());
        dest.put(data.data());
    }


    @Override
    public Event fromBytes(ByteBuffer buffer) {
        String type = strSerializer.fromBytes(buffer);
        long timestamp = buffer.getLong();
        long sequence = buffer.getLong();
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);

        return Event.load(sequence, type, data, timestamp);
    }

}
