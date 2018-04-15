package io.joshworks.fstore.serializer.arrays;

import java.nio.ByteBuffer;

public class LongArraySerializer extends FixedObjectSizeArraySerializer<long[]> {

    @Override
    public ByteBuffer toBytes(long[] data) {
        ByteBuffer bb = allocate(data.length);
        bb.asLongBuffer().put(data);
        bb.clear();
        return bb;
    }

    @Override
    public void writeTo(long[] data, ByteBuffer dest) {
        dest.putInt(data.length);
        dest.asLongBuffer().put(data);
    }

    @Override
    public long[] fromBytes(ByteBuffer data) {
        int size = getSize(data);
        long[] array = new long[size];
        data.asLongBuffer().get(array);
        return array;
    }

    @Override
    int byteSize() {
        return Long.BYTES;
    }

}
