package io.joshworks.fstore.serializer.arrays;

import java.nio.ByteBuffer;

public class ShortArraySerializer extends FixedObjectSizeArraySerializer<short[]> {

    @Override
    public ByteBuffer toBytes(short[] data) {
        ByteBuffer bb = allocate(data.length);
        bb.asShortBuffer().put(data);
        bb.clear();
        return bb;
    }

    @Override
    public short[] fromBytes(ByteBuffer data) {
        int size = getSize(data);
        short[] array = new short[size];
        data.asShortBuffer().get(array);
        return array;
    }

    @Override
    int byteSize() {
        return Short.BYTES;
    }

}
