package io.joshworks.fstore.serializer.arrays;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public abstract class ArraySerializer<T> implements Serializer<T> {

    abstract int byteSize();

    protected ByteBuffer allocate(int arrayLength) {
        ByteBuffer bb = ByteBuffer.allocate(Integer.BYTES + arrayLength * byteSize());
        return bb.putInt(arrayLength);
    }

    protected int getSize(ByteBuffer data) {
        return data.getInt();
    }

}
