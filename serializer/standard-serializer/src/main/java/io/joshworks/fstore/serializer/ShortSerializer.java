package io.joshworks.fstore.serializer;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class ShortSerializer implements Serializer<Short> {
    @Override
    public ByteBuffer toBytes(Short data) {
        return ByteBuffer.allocate(Short.BYTES).putShort(data).flip();
    }

    @Override
    public Short fromBytes(ByteBuffer data) {
        return data.getShort();
    }
}
