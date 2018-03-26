package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.DiskStorage;
import io.joshworks.fstore.core.io.MMapStorage;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.Log;
import io.joshworks.fstore.log.LogFileUtils;
import io.joshworks.fstore.log.LogSegment;
import io.joshworks.fstore.log.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class LogAppender<T> implements Log<T> {

    private static final Logger logger = LoggerFactory.getLogger(LogAppender.class);

    private final int segmentBitShift;
    final long maxSegments;

    private final File directory;
    private final Serializer<T> serializer;

    final List<Log<T>> segments;
    private Log<T> currentSegment;

    private Metadata metadata;
    private final boolean mmap;
    private long position;
    private final int segmentSize;
    private final long rollFrequency;
    private long lastRollTime = System.currentTimeMillis();

    protected LogAppender(File directory, Serializer<T> serializer, Metadata metadata) {
        this.directory = directory;
        this.serializer = serializer;
        this.mmap = metadata.mmap;
        this.position = metadata.lastPosition;
        this.segmentSize = metadata.segmentSize;
        this.rollFrequency = metadata.rollFrequency;
        this.segmentBitShift = metadata.segmentBitShift;
        this.metadata = metadata;
        this.maxSegments = (long) Math.pow(2, this.segmentBitShift);

        this.segments = loadSegments(directory, serializer);
    }

    public static <T> LogAppender<T> simpleLog(File directory, Serializer<T> serializer) {
        if (!LogFileUtils.metadataExists(directory)) {
            return createSimpleLog(new Builder<>(directory, serializer));
        }
        return openSimpleLog(directory, serializer);
    }

    static <T> LogAppender<T> createSimpleLog(Builder<T> builder) {
        LogFileUtils.createRoot(builder.directory);
        Metadata metadata = new Metadata(builder.segmentSize, builder.segmentBitShift, builder.rollFrequency, builder.mmap);
        tryCreateMetadata(builder.directory, metadata);
        LogAppender<T> appender = new LogAppender<>(builder.directory, builder.serializer, metadata);
        appender.initSegment();
        return appender;
    }

    static <T> LogAppender<T> openSimpleLog(File directory, Serializer<T> serializer) {
        Metadata metadata = LogFileUtils.readBaseMetadata(directory);
        LogAppender<T> appender = new LogAppender<>(directory, serializer, metadata);
        appender.initSegment();
        return appender;
    }

    public static <T> LogAppender<T> blockLog(File directory, Serializer<T> serializer, Codec codec) {
        if (!LogFileUtils.metadataExists(directory)) {
            return createBlockLog(new BlockSegmentBuilder<>(new Builder<>(directory, serializer), codec));
        }
        return openBlockLog(directory, serializer, codec);
    }

    static <T> CompressedBlockLogAppender<T> createBlockLog(BlockSegmentBuilder<T> blockBuilder) {
        LogFileUtils.createRoot(blockBuilder.base.directory);
        BlockAppenderMetadata metadata = BlockAppenderMetadata.of(blockBuilder);
        LogFileUtils.writeMetadata(blockBuilder.base.directory, metadata);
        CompressedBlockLogAppender<T> appender = new CompressedBlockLogAppender<>(blockBuilder.base.directory, blockBuilder.base.serializer, metadata, blockBuilder.codec);
        appender.initSegment();
        return appender;
    }

    static <T> LogAppender<T> openBlockLog(File directory, Serializer<T> serializer, Codec codec) {
        BlockAppenderMetadata metadata = LogFileUtils.readBlockMetadata(directory);
        CompressedBlockLogAppender<T> appender = new CompressedBlockLogAppender<>(directory, serializer, metadata, codec);
        appender.initSegment();
        return appender;
    }

    protected void initSegment() {
        File segmentFile = LogFileUtils.newSegmentFile(directory, maxSegments, segments.size());
        Storage storage = createStorage(segmentFile, segmentSize);
        currentSegment = createSegment(storage, serializer);
        segments.add(currentSegment);
    }

    private static void tryCreateMetadata(File directory, Metadata metadata) {
        try {
            LogFileUtils.writeMetadata(directory, metadata);
        } catch (RuntimeIOException e) {
            try {
                Files.delete(directory.toPath());
            } catch (IOException e1) {
                logger.error("Failed to revert directory creation: " + directory.getPath());
            }
            throw e;
        }
    }

    private List<Log<T>> loadSegments(final File directory, final Serializer<T> serializer) {
        return LogFileUtils.loadSegments(directory, f -> openSegment(openStorage(f), serializer, 0));
    }

    private Storage openStorage(File file) {
        if (mmap)
            return new MMapStorage(file, FileChannel.MapMode.READ_WRITE);
        return new DiskStorage(file);
    }

    private Storage createStorage(File file, long length) {
        if (mmap)
            return new MMapStorage(file, length, FileChannel.MapMode.READ_WRITE);
        return new DiskStorage(file, length);
    }

    protected Log<T> createSegment(Storage storage, Serializer<T> serializer) {
        return LogSegment.create(storage, serializer);
    }

    protected Log<T> openSegment(Storage storage, Serializer<T> serializer, long position) {
        return LogSegment.open(storage, serializer, position);
    }

    private Log<T> roll() {
        try {
            logger.info("Rolling appender");
            metadata.lastPosition(position);

            currentSegment.flush();
            //TODO what happens if it fails ?? checkIntegrity ? which one should come first ?
            LogFileUtils.writeMetadata(directory, metadata);

            File newSegmentFile = LogFileUtils.newSegmentFile(directory, maxSegments, this.segments.size());
            Storage storage = createStorage(newSegmentFile, segmentSize);
            Log<T> newSegment = createSegment(storage, serializer);
            this.segments.add(newSegment);
            this.lastRollTime = System.currentTimeMillis();
            return newSegment;
        } catch (IOException e) {
            throw new RuntimeIOException("Could not close segment file", e);
        }
    }

    int getSegment(long position) {
        int segmentIdx = (int) (position >> segmentBitShift);
        if (segmentIdx > maxSegments) {
            throw new IllegalArgumentException("Invalid segment, value cannot be greater than " + maxSegments);
        }
        return segmentIdx;
    }

    long toSegmentedPosition(long segmentIdx, long position) {
        if (segmentIdx < 0) {
            throw new IllegalArgumentException("Segment index cannot less than zero");
        }
        if (segmentIdx > maxSegments) {
            throw new IllegalArgumentException("Segment index cannot be greater than " + maxSegments);
        }
        return segmentIdx << segmentBitShift | position; //segments will always start at 1
    }

    long getPositionOnSegment(long position) {
        long mask = (1 << segmentBitShift) - 1;
        return (int) (position & mask);
    }

    private boolean shouldRoll(Log<T> currentSegment) {
        if (currentSegment.size() > segmentSize) {
            return true;
        }
        long now = System.currentTimeMillis();
        boolean expired = rollFrequency > 0 && now - lastRollTime > rollFrequency;
        return expired && currentSegment.entries() > 0;
    }

    @Override
    public long append(T data) {
        long segmentPosition = currentSegment.append(data);
        this.position = toSegmentedPosition(segments.size() - 1, segmentPosition);
        if(position < 0) {
            throw new IllegalStateException("Invalid address " + position);
        }
        if (shouldRoll(currentSegment)) {
            currentSegment = roll();
        }
        return this.position;
    }

    @Override
    public Scanner<T> scanner() {
        return new RollingSegmentReader(new LinkedList<>(segments), 0);
    }

    @Override
    public Stream<T> stream() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(scanner(), Spliterator.ORDERED), false);
    }

    @Override
    public Scanner<T> scanner(long position) {
        return new RollingSegmentReader(new LinkedList<>(segments), position);
    }

    @Override
    public long position() {
        return position;
    }

    @Override
    public T get(long position) {
        int segmentIdx = getSegment(position);
        if (segmentIdx < 0) {
            return null;
        }
        long positionOnSegment = getPositionOnSegment(position);
        Log<T> segment = segments.get(segmentIdx);
        if (segment != null) {
            return segment.get(positionOnSegment);
        }
        return null;
    }

    @Override
    public T get(long position, int length) {
        int segmentIdx = getSegment(position);
        long positionOnSegment = getPositionOnSegment(position);
        if (segmentIdx < 0) {
            return null;
        }
        Log<T> segment = segments.get(segmentIdx);
        if (segment != null) {
            return segment.get(positionOnSegment, length);
        }
        return null;

    }

    @Override
    public long entries() {
        return segments.stream().mapToLong(Log::entries).sum();
    }

    @Override
    public long size() {
        return segments.stream().mapToLong(Log::size).sum();
    }

    @Override
    public void checkIntegrity() {
        if(!segments.isEmpty()) {
            segments.get(segments.size() - 1).checkIntegrity();
        }
    }

    @Override
    public void close() throws IOException {
        metadata.lastPosition(position);
        LogFileUtils.writeMetadata(directory, metadata);
        for (Log<T> segment : segments) {
            segment.close();
        }
    }

    @Override
    public void flush() throws IOException {
        currentSegment.flush();
    }

    private class RollingSegmentReader implements Scanner<T> {

        private final List<Log<T>> segments;
        private Scanner<T> current;
        private int segmentIdx;

        public RollingSegmentReader(List<Log<T>> segments, long position) {
            this.segments = segments;
            if (!segments.isEmpty()) {
                this.segmentIdx = getSegment(position);
                long positionOnSegment = getPositionOnSegment(position);
                this.current = segments.get(segmentIdx).scanner(positionOnSegment);
            }
        }

        @Override
        public long position() {
            return toSegmentedPosition(segmentIdx, current.position());
        }

        @Override
        public Iterator<T> iterator() {
            return this;
        }

        @Override
        public boolean hasNext() {
            if (current == null) {
                return false;
            }
            boolean hasNext = current.hasNext();
            if (!hasNext) {
                if (++segmentIdx >= segments.size()) {
                    return false;
                }
                current = segments.get(segmentIdx).scanner();
                return current.hasNext();
            }
            return true;
        }

        @Override
        public T next() {
            return current.next();
        }
    }
}
