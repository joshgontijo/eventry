package io.joshworks.fstore.serializer.arrays;

import java.nio.ByteBuffer;

public class ByteArraySerializer extends FixedObjectSizeArraySerializer<byte[]> {

    @Override
    public ByteBuffer toBytes(byte[] data) {
        ByteBuffer bb = allocate(data.length);
        bb.put(data);
        return (ByteBuffer) bb.flip();
    }

    @Override
    public void writeTo(byte[] data, ByteBuffer dest) {
        dest.putInt(data.length);
        dest.put(data);
    }

    @Override
    public byte[] fromBytes(ByteBuffer data) {
        byte[] b = new byte[data.remaining()];
        data.get(b);
        return b;
    }

    @Override
    int byteSize() {
        return Byte.BYTES;
    }
}
