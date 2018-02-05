package io.joshworks.fstore.serializer.arrays;

import java.nio.ByteBuffer;

public class ByteArraySerializer extends ArraySerializer<byte[]> {

    @Override
    public ByteBuffer toBytes(byte[] data) {
        ByteBuffer bb = allocate(data.length);
        bb.put(data);
        bb.clear();
        return bb;
    }

    @Override
    public byte[] fromBytes(ByteBuffer data) {
        int size = getSize(data);
        byte[] array = new byte[size];
        data.get(array);
        return array;
    }

    @Override
    int byteSize() {
        return Byte.BYTES;
    }

}
