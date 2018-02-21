package io.joshworks.fstore.serializer;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class StringSerializer implements Serializer<String> {

    @Override
    public ByteBuffer toBytes(String data) {
        return ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public String fromBytes(ByteBuffer buffer) {
        if(!buffer.hasArray()) {
            byte[] data = new byte[buffer.remaining()];
            buffer.get(data);
            return new String(data, StandardCharsets.UTF_8);
        }
        return new String(buffer.array(), buffer.position(), buffer.remaining(), StandardCharsets.UTF_8);
    }
}
