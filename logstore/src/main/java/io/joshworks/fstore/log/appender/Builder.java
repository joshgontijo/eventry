package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.DataReader;
import io.joshworks.fstore.log.BitUtil;
import io.joshworks.fstore.log.appender.naming.NamingStrategy;
import io.joshworks.fstore.log.appender.naming.UUIDNamingStrategy;
import io.joshworks.fstore.log.reader.FixedBufferDataReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Objects;

public final class Builder<T> {

    private static final Logger logger = LoggerFactory.getLogger(Builder.class);

    //How many bits a segment index can hold
    private static final int SEGMENT_BITS = 18;

    final File directory;
    final Serializer<T> serializer;
    DataReader reader = new FixedBufferDataReader(false, 1);
    NamingStrategy namingStrategy = new UUIDNamingStrategy();

    int segmentBitShift = Long.SIZE - SEGMENT_BITS;
    int segmentSize = 10485760; //10mb
    boolean mmap;
    boolean asyncFlush;
    private int blockSize = 4096; //only for block appender

    Builder(File directory, Serializer<T> serializer) {
        Objects.requireNonNull(directory, "directory cannot be null");
        Objects.requireNonNull(serializer, "serializer cannot be null");
        this.directory = directory;
        this.serializer = serializer;
    }

    public Builder<T> segmentSize(int size) {
        long maxAddress = BitUtil.maxValueForBits(segmentBitShift);
        if (size > maxAddress) {
            throw new IllegalArgumentException("Maximum position allowed is " + maxAddress);
        }
        this.segmentSize = size;
        return this;
    }

    public Builder<T> namingStrategy(NamingStrategy strategy) {
        Objects.requireNonNull(strategy, "NamingStrategy must be provided");
        this.namingStrategy = strategy;
        return this;
    }

    public Builder<T> reader(DataReader reader) {
        Objects.requireNonNull(reader, "reader must no be null");
        this.reader = reader;
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

    public Builder<T> blockSize(int blockSize) {
        if(blockSize < 4096) {
            throw new IllegalArgumentException("BlockSize must be greater or equals than 4096 bytes");
        }
        this.blockSize = blockSize;
        return this;
    }



}
