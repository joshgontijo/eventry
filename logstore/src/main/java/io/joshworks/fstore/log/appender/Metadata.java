package io.joshworks.fstore.log.appender;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Metadata {

    final int segmentSize;
    final int segmentBitShift;
    final long rollFrequency;
    final boolean mmap;
    long lastPosition;
    long entryCount;

    public Metadata(int segmentSize, int segmentBitShift, long rollFrequency, boolean mmap) {
        this.segmentSize = segmentSize;
        this.segmentBitShift = segmentBitShift;
        this.rollFrequency = rollFrequency;
        this.mmap = mmap;
    }

    public static <T> Metadata of(Builder<T> builder) {
        return new Metadata(builder.segmentSize, builder.segmentBitShift, builder.rollFrequency, builder.mmap);
    }

    public static Metadata of(DataInput in) throws IOException {
        long lastPosition = in.readLong();
        int segmentSize = in.readInt();
        int segmentBitShift = in.readInt();
        long entryCount = in.readLong();
        long rollFrequency = in.readLong();
        boolean mmap = in.readBoolean();

        return new Metadata(segmentSize, segmentBitShift, rollFrequency, mmap)
                .entryCount(entryCount)
                .lastPosition(lastPosition);

    }

    public void writeTo(DataOutput out) throws IOException {
        out.writeLong(lastPosition);
        out.writeInt(segmentSize);
        out.writeInt(segmentBitShift);
        out.writeLong(entryCount);
        out.writeLong(rollFrequency);
        out.writeBoolean(mmap);
    }

    public Metadata lastPosition(final long lastPosition) {
        this.lastPosition = lastPosition;
        return this;
    }

    public Metadata entryCount(final long entryCount) {
        this.entryCount = entryCount;
        return this;
    }

}
