package io.joshworks.fstore.serializer.arrays;

import java.nio.ByteBuffer;

public class FloatArraySerializer extends ArraySerializer<float[]> {

    @Override
    public ByteBuffer toBytes(float[] data) {
        ByteBuffer bb = allocate(data.length);
        bb.asFloatBuffer().put(data);
        bb.clear();
        return bb;
    }

    @Override
    public float[] fromBytes(ByteBuffer data) {
        int size = getSize(data);
        float[] array = new float[size];
        data.asFloatBuffer().get(array);
        return array;
    }

    @Override
    int byteSize() {
        return Float.BYTES;
    }

}
