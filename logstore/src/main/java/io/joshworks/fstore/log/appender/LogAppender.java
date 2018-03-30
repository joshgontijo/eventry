package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.DiskStorage;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.MMapStorage;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.BitUtil;
import io.joshworks.fstore.log.Log;
import io.joshworks.fstore.log.LogFileUtils;
import io.joshworks.fstore.log.LogSegment;
import io.joshworks.fstore.log.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Position address schema
 * <p>
 * 64bits
 * <p>
 * Simple segment
 * [SEGMENT_IDX] [POSITION_ON_SEGMENT]
 */
public class LogAppender<T> implements Log<T> {

    private static final Logger logger = LoggerFactory.getLogger(LogAppender.class);

    private final File directory;
    private final Serializer<T> serializer;
    private final boolean mmap;
    private final Metadata metadata;
    private final State state;

    final long maxSegments;
    final long maxAddressPerSegment;

    final List<Log<T>> segments = new LinkedList<>();
    Log<T> currentSegment;
    private long lastRollTime = System.currentTimeMillis();

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    protected LogAppender(File directory, Serializer<T> serializer, Metadata metadata, State state) {
        this.directory = directory;
        this.serializer = serializer;
        this.mmap = metadata.mmap;
        this.state = state;
        this.metadata = metadata;
        this.maxSegments = BitUtil.maxValueForBits(Long.SIZE - metadata.segmentBitShift);
        this.maxAddressPerSegment = BitUtil.maxValueForBits(metadata.segmentBitShift);

        if (metadata.segmentBitShift >= Long.SIZE || metadata.segmentBitShift < 0) {
            //just a numeric validation, values near 64 and 0 are still nonsense
            throw new IllegalArgumentException("segmentBitShift must be between 0 and " + Long.SIZE);
        }

        logger.info("SEGMENT BIT SHIFT: {}", metadata.segmentBitShift);
        logger.info("MAX SEGMENTS: {} ({} bits)", maxSegments, Long.SIZE - metadata.segmentBitShift);
        logger.info("MAX ADDRESS PER SEGMENT: {} ({} bits)", maxAddressPerSegment, metadata.segmentBitShift);

        this.scheduler.scheduleAtFixedRate(() -> LogFileUtils.writeState(directory, state), 5, 1, TimeUnit.SECONDS);
    }

    public static <T> LogAppender<T> simpleLog(Builder<T> builder) {

        if (!LogFileUtils.metadataExists(builder.directory)) {
            return createSimpleLog(builder);
        }
        return openSimpleLog(builder);
    }

    public static <T> LogAppender<T> blockLog(BlockSegmentBuilder<T> builder) {
        if (!LogFileUtils.metadataExists(builder.base.directory)) {
            return createBlockLog(builder);
        }
        return openBlockLog(builder);
    }

    private static <T> LogAppender<T> createSimpleLog(Builder<T> builder) {
        logger.info("Creating contiguous LogAppender");

        LogFileUtils.createRoot(builder.directory);
        Metadata metadata = new Metadata(builder.segmentSize, builder.segmentBitShift, builder.rollFrequency, builder.mmap, builder.asyncFlush);
        LogFileUtils.tryCreateMetadata(builder.directory, metadata);
        LogAppender<T> appender = new LogAppender<>(builder.directory, builder.serializer, metadata, State.empty());
        appender.initSegment();
        return appender;
    }

    private static <T> LogAppender<T> openSimpleLog(Builder<T> builder) {
        logger.info("Opening contiguous LogAppender");

        File directory = builder.directory;
        Serializer<T> serializer = builder.serializer;

        Metadata metadata = LogFileUtils.readBaseMetadata(directory);
        State state = LogFileUtils.readState(directory);
        LogAppender<T> appender = new LogAppender<>(directory, serializer, metadata, state);
        appender.loadSegments(directory, serializer, state);
        appender.loadCurrentSegment();
        return appender;
    }

    private static <T> BlockCompressedLogAppender<T> createBlockLog(BlockSegmentBuilder<T> blockBuilder) {
        LogFileUtils.createRoot(blockBuilder.base.directory);
        BlockAppenderMetadata metadata = BlockAppenderMetadata.of(blockBuilder);
        LogFileUtils.writeMetadata(blockBuilder.base.directory, metadata);
        BlockCompressedLogAppender<T> appender = new BlockCompressedLogAppender<>(
                blockBuilder.base.directory,
                blockBuilder.base.serializer,
                metadata, State.empty(),
                blockBuilder.codec
        );

        appender.initSegment();
        return appender;
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
    private static <T> LogAppender<T> openBlockLog(BlockSegmentBuilder<T> blockBuilder) {
        logger.info("Opening block LogAppender");

        File directory = blockBuilder.base.directory;
        Serializer<T> serializer = blockBuilder.base.serializer;
        Codec codec = blockBuilder.codec;

        BlockAppenderMetadata metadata = LogFileUtils.readBlockMetadata(directory);
        State state = LogFileUtils.readState(directory);

        BlockCompressedLogAppender<T> appender = new BlockCompressedLogAppender<>(directory, serializer, metadata, state, codec);
        appender.loadSegments(directory, serializer, state);
        appender.loadCurrentSegment();
        return appender;
    }

    protected void initSegment() {
        File segmentFile = LogFileUtils.newSegmentFile(directory, maxSegments, segments.size());
        Storage storage = createStorage(segmentFile, metadata.segmentSize);
        currentSegment = createSegment(storage, serializer);
        segments.add(currentSegment);

        state.segments.add(currentSegment.name());
        LogFileUtils.writeState(directory, state);

    }

    protected void loadCurrentSegment() {
        if (segments.isEmpty()) {
            throw new IllegalStateException("No segment available");
        }
        currentSegment = segments.get(segments.size() - 1);
        state.position = Log.checkIntegrity(state.position, currentSegment);
        LogFileUtils.writeState(directory, state);
    }


    protected void loadSegments(final File directory, final Serializer<T> serializer, final State state) {
        List<File> segmentFiles = LogFileUtils.loadSegments(directory);

        for (int i = 0; i < segmentFiles.size(); i++) {
            File segmentFile = segmentFiles.get(i);
            if (!state.segments.contains(segmentFile.getName())) {
                logger.warn("Segment file '{}' not present in the state, ignoring", segmentFile.getName());
                continue;
            }

            Storage storage = openStorage(segmentFile);
            if (i == segmentFiles.size() - 1) { //last, open in write mode
                Log<T> log = openSegment(storage, serializer, state.position);
                segments.add(log);
            } else { // read-only
                Log<T> log = openSegment(storage, serializer, 0);
                segments.add(log);
            }
        }
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
            flush();

            File newSegmentFile = LogFileUtils.newSegmentFile(directory, maxSegments, this.segments.size());
            Storage storage = createStorage(newSegmentFile, metadata.segmentSize);
            Log<T> newSegment = createSegment(storage, serializer);
            this.segments.add(newSegment);

            state.segments.add(newSegment.name());
            LogFileUtils.writeState(directory, state);

            this.lastRollTime = System.currentTimeMillis();
            return newSegment;
        } catch (IOException e) {
            throw new RuntimeIOException("Could not close segment file", e);
        }
    }

    int getSegment(long position) {
        long segmentIdx = (position >>> metadata.segmentBitShift);
        if (segmentIdx > maxSegments) {
            throw new IllegalArgumentException("Invalid segment, value cannot be greater than " + maxSegments);
        }
        if (segmentIdx > segments.size()) {
            throw new IllegalArgumentException("No segment for address " + position + " (segmentIdx: " + segmentIdx + "), available segments: " + segments.size());
        }
        return (int) segmentIdx;
    }

    long toSegmentedPosition(long segmentIdx, long position) {
        if (segmentIdx < 0) {//segments will always start at 1
            throw new IllegalArgumentException("Segment index must be greater than zero");
        }
        if (segmentIdx > maxSegments) {
            throw new IllegalArgumentException("Segment index cannot be greater than " + maxSegments);
        }
        return (segmentIdx << metadata.segmentBitShift) | position;
    }

    long getPositionOnSegment(long position) {
        long mask = (1 << metadata.segmentBitShift) - 1;
        return (int) (position & mask);
    }

    private boolean shouldRoll(Log<T> currentSegment) {
        if (currentSegment.size() > metadata.segmentSize) {
            return true;
        }
        long now = System.currentTimeMillis();
        boolean expired = metadata.rollFrequency > 0 && now - lastRollTime > metadata.rollFrequency;
        return expired && currentSegment.size() > 0;
    }

    @Override
    public long append(T data) {
        long positionOnSegment = currentSegment.append(data);
        long segmentedPosition = toSegmentedPosition(segments.size() - 1, positionOnSegment);
        if (positionOnSegment < 0) {
            throw new IllegalStateException("Invalid address " + positionOnSegment);
        }
        if (shouldRoll(currentSegment)) {
            currentSegment = roll();
        }
        state.position = currentSegment.position();
        state.entryCount++;
        return segmentedPosition;
    }

    @Override
    public String name() {
        return directory.getName();
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
        return state.position;
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
    public long size() {
        return segments.stream().mapToLong(Log::size).sum();
    }

    @Override
    public void close() throws IOException {
        scheduler.shutdown();
        for (Log<T> segment : segments) {
            IOUtils.closeQuietly(segment);
        }
        state.position = currentSegment.position();
        LogFileUtils.writeState(directory, state);
    }

    @Override
    public void flush() throws IOException {
        if (metadata.asyncFlush) {
            executor.execute(() -> {
                try {
                    currentSegment.flush();
                } catch (IOException e) {
                    throw RuntimeIOException.of(e);
                }
            });

        } else {
            currentSegment.flush();
        }

    }

    public List<String> segments() {
        return segments.stream().map(Log::name).collect(Collectors.toList());
    }

    public long entries() {
        return this.state.entryCount;
    }

    private class RollingSegmentReader extends Scanner<T> {

        private final List<Log<T>> segments;
        private Scanner<T> current;
        private int segmentIdx;

        public RollingSegmentReader(List<Log<T>> segments, long position) {
            super(null, null, position);
            this.segments = segments;
            this.segmentIdx = getSegment(position);
            long positionOnSegment = getPositionOnSegment(position);
            this.current = segments.get(segmentIdx).scanner(positionOnSegment);
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
        protected T readAndVerify() {
            return null;
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
