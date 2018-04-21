package io.joshworks.fstore.log.appender;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Metadata {

    final int segmentSize;
    final int segmentBitShift;
    final boolean mmap;
    final boolean asyncFlush;

    Metadata(int segmentSize, int segmentBitShift, boolean mmap, boolean asyncFlush) {
        this.segmentSize = segmentSize;
        this.segmentBitShift = segmentBitShift;
        this.mmap = mmap;
        this.asyncFlush = asyncFlush;
    }

    public static <T> Metadata readFrom(Builder<T> builder) {
        return new Metadata(builder.segmentSize, builder.segmentBitShift, builder.mmap, builder.asyncFlush);
    }

    public static Metadata readFrom(DataInput in) throws IOException {
        int segmentSize = in.readInt();
        int segmentBitShift = in.readInt();
        boolean mmap = in.readBoolean();
        boolean asyncFlush = in.readBoolean();

        return new Metadata(segmentSize, segmentBitShift, mmap, asyncFlush);
    }

    public void writeTo(DataOutput out) throws IOException {
        out.writeInt(segmentSize);
        out.writeInt(segmentBitShift);
        out.writeBoolean(mmap);
        out.writeBoolean(asyncFlush);
    }


}
