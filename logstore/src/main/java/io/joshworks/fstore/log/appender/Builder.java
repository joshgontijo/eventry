package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.DataReader;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.BitUtil;
import io.joshworks.fstore.log.appender.merge.ConcatenateCombiner;
import io.joshworks.fstore.log.appender.merge.SegmentCombiner;
import io.joshworks.fstore.log.appender.naming.NamingStrategy;
import io.joshworks.fstore.log.appender.naming.ShortUUIDNamingStrategy;
import io.joshworks.fstore.log.reader.FixedBufferDataReader;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.LogSegment;
import io.joshworks.fstore.log.segment.Type;
import io.joshworks.fstore.log.segment.block.Block;
import io.joshworks.fstore.log.segment.block.BlockSegment;
import io.joshworks.fstore.log.segment.block.DefaultBlockSegment;

import java.io.File;
import java.util.Objects;

public class Builder<T> {

    //How many bits a segment index can hold
    private static final int SEGMENT_BITS = 18;

    final File directory;
    final Serializer<T> serializer;
    DataReader reader = new FixedBufferDataReader(false, 1);
    NamingStrategy namingStrategy = new ShortUUIDNamingStrategy();
    SegmentCombiner<T> combiner = new ConcatenateCombiner<>();

    int segmentBitShift = Long.SIZE - SEGMENT_BITS;
    int segmentSize = 10485760; //10mb
    boolean mmap;
    boolean asyncFlush;
    int maxSegmentsPerLevel = 3;
    int mmapBufferSize = segmentSize;

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

    public Builder<T> maxSegmentsPerLevel(int maxSegmentsPerLevel) {
        if(maxSegmentsPerLevel <= 0) {
            throw new IllegalArgumentException("maxSegmentsPerLevel must be greater than zero");
        }
        this.maxSegmentsPerLevel = maxSegmentsPerLevel;
        return this;
    }

    public Builder<T> disableCompaction() {
        this.maxSegmentsPerLevel = LogAppender.COMPACTION_DISABLED;
        return this;
    }

    public Builder<T> namingStrategy(NamingStrategy strategy) {
        Objects.requireNonNull(strategy, "NamingStrategy must be provided");
        this.namingStrategy = strategy;
        return this;
    }

    public Builder<T> compactionStrategy(SegmentCombiner<T> combiner) {
        Objects.requireNonNull(combiner, "SegmentCombiner must be provided");
        this.combiner = combiner;
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

    public Builder<T> mmap(int bufferSize) {
        this.mmap = true;
        this.mmapBufferSize = bufferSize;
        return this;
    }

    public Builder<T> asyncFlush() {
        this.asyncFlush = true;
        return this;
    }

    public LogAppender<T, LogSegment<T>> simple() {
        return new LogAppender<>(this, new SegmentFactory<T, LogSegment<T>>() {
            @Override
            public LogSegment<T> createOrOpen(Storage storage, Serializer<T> serializer, DataReader reader, Type type) {
                return new LogSegment<>(storage, serializer, reader, type);
            }
        });
    }

    public <L extends Log<T>> LogAppender<T, L> simple(SegmentFactory<T, L> factory) {
        return new LogAppender<>(this, factory);
    }

    public LogAppender<T, DefaultBlockSegment<T>> block(int maxBlockSize) {
        return new LogAppender<>(this, new SegmentFactory<T, DefaultBlockSegment<T>>() {
            @Override
            public DefaultBlockSegment<T> createOrOpen(Storage storage, Serializer<T> serializer, DataReader reader, Type type) {
                return new DefaultBlockSegment<>(storage, serializer, reader, type, maxBlockSize);
            }
        });
    }

    public <B extends Block<T>, L extends BlockSegment<T, B>> LogAppender<T, L> block(SegmentFactory<T, L> factory) {
        return new LogAppender<>(this, factory);
    }

}
