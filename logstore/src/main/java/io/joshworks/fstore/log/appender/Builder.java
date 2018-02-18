package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.Serializer;

import java.io.File;
import java.time.Duration;
import java.util.Objects;

public final class Builder<T> {

    final File directory;
    final Serializer<T> serializer;

    int segmentBitShift = 28;
    int segmentSize;
    boolean mmap;
    long rollFrequency = -1; //never

    public Builder(File directory, Serializer<T> serializer) {
        Objects.requireNonNull(directory, "directory cannot be null");
        Objects.requireNonNull(serializer, "serializer cannot be null");
        this.directory = directory;
        this.serializer = serializer;
    }

    //TODO add minimal / max validation (max based on bitShift)
    public Builder<T> segmentSize(int size) {
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
}
