package io.joshworks.fstore.serializer;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class DoubleSerializer implements Serializer<Double> {
    @Override
    public ByteBuffer toBytes(Double data) {
        return (ByteBuffer) ByteBuffer.allocate(Double.BYTES).putDouble(data).flip();
    }

    @Override
    public void writeTo(Double data, ByteBuffer dest) {
        dest.putDouble(data);
    }

    @Override
    public Double fromBytes(ByteBuffer data) {
        return data.getDouble();
    }
}
