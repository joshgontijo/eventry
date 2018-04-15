package io.joshworks.fstore.serializer;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class IntegerSerializer implements Serializer<Integer> {

    @Override
    public ByteBuffer toBytes(Integer data) {
        return (ByteBuffer) ByteBuffer.allocate(Integer.BYTES).putInt(data).flip();
    }

    @Override
    public void writeTo(Integer data, ByteBuffer dest) {
        dest.putInt(data);
    }

    @Override
    public Integer fromBytes(ByteBuffer data) {
        return data.getInt();
    }
}
