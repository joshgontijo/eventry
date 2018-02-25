package io.joshworks.fstore.serializer;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class ByteBufferCopy implements Serializer<ByteBuffer> {

    @Override
    public ByteBuffer toBytes(ByteBuffer data) {
        return copy(data);
    }

    @Override
    public ByteBuffer fromBytes(ByteBuffer buffer) {
        return copy(buffer);
    }

    private ByteBuffer copy(ByteBuffer data) {
        byte[] bytes = new byte[data.remaining()];
        data.get(bytes);
        return ByteBuffer.wrap(bytes);
    }
}
