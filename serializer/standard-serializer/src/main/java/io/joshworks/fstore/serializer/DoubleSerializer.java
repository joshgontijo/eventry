package io.joshworks.fstore.serializer;

import io.joshworks.fstore.api.Serializer;

import java.nio.ByteBuffer;

public class DoubleSerializer implements Serializer<Double> {
    @Override
    public ByteBuffer toBytes(Double data) {
        return ByteBuffer.allocate(Double.BYTES).putDouble(data).flip();
    }

    @Override
    public Double fromBytes(ByteBuffer data) {
        return data.getDouble();
    }
}
