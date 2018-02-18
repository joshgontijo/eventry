package io.joshworks.fstore.serializer;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

class IntegerSerializer implements Serializer<Integer> {

    @Override
    public ByteBuffer toBytes(Integer data) {
        return (ByteBuffer) ByteBuffer.allocate(Integer.BYTES).putInt(data).flip();
    }

    @Override
    public Integer fromBytes(ByteBuffer data) {
        return data.getInt();
    }
}
