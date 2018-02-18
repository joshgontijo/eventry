package io.joshworks.fstore.serializer;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class BooleanSerializer implements Serializer<Boolean> {

    @Override
    public ByteBuffer toBytes(Boolean data) {
        //wasting a lot of space here
        return (ByteBuffer) ByteBuffer.allocate(1).put((byte) (data ? 1 : 0)).flip();
    }

    @Override
    public Boolean fromBytes(ByteBuffer data) {
        return data.get() == (byte) 1;
    }
}
