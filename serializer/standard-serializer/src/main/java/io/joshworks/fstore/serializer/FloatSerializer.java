package io.joshworks.fstore.serializer;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class FloatSerializer implements Serializer<Float> {

    @Override
    public ByteBuffer toBytes(Float data) {
        return ByteBuffer.allocate(Float.BYTES).putFloat(data).flip();
    }

    @Override
    public Float fromBytes(ByteBuffer data) {
        return data.getFloat();
    }
}
