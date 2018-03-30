package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.log.BitUtil;

import java.io.File;
import java.time.Duration;
import java.util.Objects;

public final class Builder<T> {

    //How many bits a segment index can hold
    private static final int SEGMENT_BITS = 18;

    final File directory;
    final Serializer<T> serializer;

    int segmentBitShift = Long.SIZE - SEGMENT_BITS;
    int segmentSize = 1073741824;
    boolean mmap;
    long rollFrequency = -1; //never
    boolean asyncFlush;

    public Builder(File directory, Serializer<T> serializer) {
        Objects.requireNonNull(directory, "directory cannot be null");
        Objects.requireNonNull(serializer, "serializer cannot be null");
        this.directory = directory;
        this.serializer = serializer;
    }

    public Builder<T> segmentSize(int size) {
        long maxAddress = BitUtil.maxValueForBits(segmentBitShift);
        if(size > maxAddress) {
            throw new IllegalArgumentException("Maximum size allowed is " + maxAddress);
        }
        this.segmentSize = size;
        return this;
    }

    public Builder<T> rollInterval(Duration interval) {
        this.rollFrequency = interval.toMillis();
        return this;
    }

    public Builder<T> mmap() {
        this.mmap = true;
        return this;
    }

    public Builder<T> asyncFlush() {
        this.asyncFlush = true;
        return this;
    }

}
