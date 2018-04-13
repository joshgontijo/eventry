package io.joshworks.fstore.es;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.VStringSerializer;

import java.nio.ByteBuffer;

public class EventSerializer implements Serializer<Event> {

    private final VStringSerializer serializer = new VStringSerializer();

    @Override
    public ByteBuffer toBytes(Event data) {

        ByteBuffer bb1 = serializer.toBytes(data.type);
        ByteBuffer bb2 = serializer.toBytes(data.data);

        ByteBuffer b = ByteBuffer.allocate(bb1.limit() + bb2.limit());

        return (ByteBuffer) b.put(bb1).put(bb2).flip();
    }

    @Override
    public Event fromBytes(ByteBuffer buffer) {
        String type = serializer.fromBytes(buffer);
        String data = serializer.fromBytes(buffer);

        return new Event(type, data);
    }
}
