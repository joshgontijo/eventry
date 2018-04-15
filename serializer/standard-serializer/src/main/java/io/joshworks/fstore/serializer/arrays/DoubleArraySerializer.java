package io.joshworks.fstore.serializer.arrays;

import java.nio.ByteBuffer;

public class DoubleArraySerializer extends FixedObjectSizeArraySerializer<double[]> {

    @Override
    public ByteBuffer toBytes(double[] data) {
        ByteBuffer bb = allocate(data.length);
        bb.asDoubleBuffer().put(data);
        bb.clear();
        return bb;
    }

    @Override
    public void writeTo(double[] data, ByteBuffer dest) {
        dest.putInt(data.length);
        dest.asDoubleBuffer().put(data);
    }

    @Override
    public double[] fromBytes(ByteBuffer data) {
        int size = getSize(data);
        double[] array = new double[size];
        data.asDoubleBuffer().get(array);
        return array;
    }

    @Override
    int byteSize() {
        return Double.BYTES;
    }

}
