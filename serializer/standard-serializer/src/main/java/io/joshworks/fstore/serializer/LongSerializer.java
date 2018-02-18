package io.joshworks.fstore.serializer;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class LongSerializer implements Serializer<Long> {
    @Override
    public ByteBuffer toBytes(Long data) {
        return (ByteBuffer) ByteBuffer.allocate(Long.BYTES).putLong(data).flip();
    }

    @Override
    public Long fromBytes(ByteBuffer data) {
        return data.getLong();
    }
}
