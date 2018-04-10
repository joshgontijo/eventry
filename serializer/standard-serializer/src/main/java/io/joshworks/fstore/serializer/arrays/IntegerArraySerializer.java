package io.joshworks.fstore.serializer.arrays;

import java.nio.ByteBuffer;

public class IntegerArraySerializer extends FixedObjectSizeArraySerializer<int[]> {

    @Override
    public ByteBuffer toBytes(int[] data) {
        ByteBuffer bb = allocate(data.length);
        bb.asIntBuffer().put(data);
        bb.clear();
        return bb;
    }

    @Override
    public int[] fromBytes(ByteBuffer data) {
        int size = getSize(data);
        int[] array = new int[size];
        data.asIntBuffer().get(array);
        data.position(data.position() + array.length * byteSize());
        return array;
    }

    @Override
    int byteSize() {
        return Integer.BYTES;
    }

}
