package io.joshworks.fstore.serializer;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class DirectSerializer implements Serializer<ByteBuffer> {

    @Override
    public ByteBuffer toBytes(ByteBuffer data) {
        return data;
    }

    @Override
    public ByteBuffer fromBytes(ByteBuffer buffer) {
        return buffer;
    }
}
