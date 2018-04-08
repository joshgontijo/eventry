package io.joshworks.fstore.serializer.arrays;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class ByteArraySerializer implements Serializer<byte[]> {

    @Override
    public ByteBuffer toBytes(byte[] data) {
        return ByteBuffer.wrap(data);
    }

    @Override
    public byte[] fromBytes(ByteBuffer data) {
        if(data.isDirect()) {
            byte[] b = new byte[data.remaining()];
            data.get(b);
            return b;
        }
        return data.array();
    }

}
