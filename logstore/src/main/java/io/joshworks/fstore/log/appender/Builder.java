package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.DataReader;
import io.joshworks.fstore.log.BitUtil;
import io.joshworks.fstore.log.LogFileUtils;
import io.joshworks.fstore.log.appender.naming.NamingStrategy;
import io.joshworks.fstore.log.appender.naming.TimestampNamingStrategy;
import io.joshworks.fstore.log.reader.FixedBufferDataReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.function.Function;

public final class Builder<T> {

    private static final Logger logger = LoggerFactory.getLogger(Builder.class);

    //How many bits a segment index can hold
    private static final int SEGMENT_BITS = 18;

    private final File directory;
    private final Serializer<T> serializer;
    private DataReader reader = new FixedBufferDataReader(false, 1);

    int segmentBitShift = Long.SIZE - SEGMENT_BITS;
    int segmentSize = 1073741824;
    boolean mmap;
    boolean asyncFlush;
    int headerSize;
    private NamingStrategy namingStrategy = new TimestampNamingStrategy();

    Builder(File directory, Serializer<T> serializer) {
        Objects.requireNonNull(directory, "directory cannot be null");
        Objects.requireNonNull(serializer, "serializer cannot be null");
        this.directory = directory;
        this.serializer = serializer;
    }

    public Builder<T> segmentSize(int size) {
        long maxAddress = BitUtil.maxValueForBits(segmentBitShift);
        if (size > maxAddress) {
            throw new IllegalArgumentException("Maximum size allowed is " + maxAddress);
        }
        this.segmentSize = size;
        return this;
    }

    public Builder<T> headerWriter(int headerSize, Function<LogAppender, ByteBuffer> dataSupplier) {
        if (headerSize < 0) {
            throw new IllegalArgumentException("Size must be equal or greater than zero");
        }
        this.headerSize = headerSize;
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


    public LogAppender<T> open() {
        if (!LogFileUtils.metadataExists(directory)) {
            return createSimpleLog();
        }
        return openSimpleLog();
    }

    private LogAppender<T> createSimpleLog() {
        logger.info("Creating LogAppender");

        LogFileUtils.createRoot(directory);
        Metadata metadata = new Metadata(segmentSize, segmentBitShift, mmap, asyncFlush);
        LogFileUtils.tryCreateMetadata(directory, metadata);
        return new LogAppender<>(directory, serializer, metadata, State.empty(), reader, namingStrategy);
    }

    //FIXME - ON PREVIOUSLY HALTED
    //java.io.EOFException
    //	at java.io.DataInputStream.readFully(DataInputStream.java:197)
    //	at java.io.DataInputStream.readLong(DataInputStream.java:416)
    //	at io.joshworks.fstore.log.appender.State.readFrom(State.java:23)
    //	at io.joshworks.fstore.log.LogFileUtils.readState(LogFileUtils.java:133)
    //	at io.joshworks.fstore.log.appender.LogAppender.openSimpleLog(LogAppender.java:93)
    //	at io.joshworks.fstore.log.appender.LogAppender.simpleLog(LogAppender.java:65)
    //	at io.joshworks.fstore.benchmark.LogAppenderBench.segmentAppender(LogAppenderBench.java:32)
    //	at io.joshworks.fstore.benchmark.LogAppenderBench.main(LogAppenderBench.java:26)
    //Exception in thread "main" io.joshworks.fstore.core.RuntimeIOException
    //	at io.joshworks.fstore.core.RuntimeIOException.of(RuntimeIOException.java:17)
    //	at io.joshworks.fstore.log.LogFileUtils.readState(LogFileUtils.java:135)
    //	at io.joshworks.fstore.log.appender.LogAppender.openSimpleLog(LogAppender.java:93)
    //	at io.joshworks.fstore.log.appender.LogAppender.simpleLog(LogAppender.java:65)
    //	at io.joshworks.fstore.benchmark.LogAppenderBench.segmentAppender(LogAppenderBench.java:32)
    //	at io.joshworks.fstore.benchmark.LogAppenderBench.main(LogAppenderBench.java:26)
    private LogAppender<T> openSimpleLog() {
        logger.info("Opening LogAppender");

        Metadata metadata = LogFileUtils.readBaseMetadata(directory);
        State state = LogFileUtils.readState(directory);
        return new LogAppender<>(directory, serializer, metadata, state, reader, namingStrategy);
    }


}
