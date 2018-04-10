package io.joshworks.fstore.serializer.arrays;

import java.nio.ByteBuffer;

public class BooleanArraySerializer extends FixedObjectSizeArraySerializer<boolean[]> {

    @Override
    public ByteBuffer toBytes(boolean[] data) {
        ByteBuffer bb = allocate(data.length);
        //TODO improve this
        for (boolean aData : data) {
            bb.put((byte) (aData ? 1 : 0));
        }
        bb.clear();
        return bb;
    }

    @Override
    public boolean[] fromBytes(ByteBuffer data) {
        int size = getSize(data);
        //TODO improve this
        boolean[] array = new boolean[size];
        for (int i = 0; i < array.length; i++) {
            array[i] = data.get() == (byte) 1;
        }
        return array;
    }

    @Override
    int byteSize() {
        return Byte.BYTES;
    }

}
