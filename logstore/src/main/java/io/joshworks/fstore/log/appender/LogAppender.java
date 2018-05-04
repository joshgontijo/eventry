package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.DataReader;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.MMapStorage;
import io.joshworks.fstore.core.io.RafStorage;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.BitUtil;
import io.joshworks.fstore.log.Log;
import io.joshworks.fstore.log.LogFileUtils;
import io.joshworks.fstore.log.Scanner;
import io.joshworks.fstore.log.appender.merge.SegmentCombiner;
import io.joshworks.fstore.log.appender.naming.NamingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Position address schema
 * <p>
 * |------------ 64bits -------------|
 * [SEGMENT_IDX] [POSITION_ON_SEGMENT]
 */
public abstract class LogAppender<T, L extends Log<T>> implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(LogAppender.class);

    private static final double SEGMENT_EXTRA_SIZE = 0.1;

    private final File directory;
    private final Serializer<T> serializer;
    private final boolean mmap;
    private final Metadata metadata;
    private final DataReader dataReader;
    private final NamingStrategy namingStrategy;

    final long maxSegments;
    final long maxAddressPerSegment;

    final LinkedList<L> rolledSegments;
    private L currentSegment;

    //state
    private State state;

    private AtomicBoolean closed = new AtomicBoolean();

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService executor = Executors.newFixedThreadPool(3);

    protected LogAppender(Builder<T> builder) {

        this.directory = builder.directory;
        this.serializer = builder.serializer;
        this.mmap = builder.mmap;
        this.dataReader = builder.reader;
        this.namingStrategy = builder.namingStrategy;

        if (!LogFileUtils.metadataExists(directory)) {
            logger.info("Creating LogAppender");

            LogFileUtils.createRoot(directory);
            Metadata newMetadata = new Metadata(builder.segmentSize, builder.segmentBitShift, builder.mmap, builder.asyncFlush);
            LogFileUtils.tryCreateMetadata(directory, newMetadata);
            this.metadata = newMetadata;
            this.state = State.empty();
        } else {
            logger.info("Opening LogAppender");
            this.metadata = LogFileUtils.readBaseMetadata(directory);
            this.state = LogFileUtils.readState(directory);
        }


        this.maxSegments = BitUtil.maxValueForBits(Long.SIZE - metadata.segmentBitShift);
        this.maxAddressPerSegment = BitUtil.maxValueForBits(metadata.segmentBitShift);

        if (metadata.segmentBitShift >= Long.SIZE || metadata.segmentBitShift < 0) {
            //just a numeric validation, values near 64 and 0 are still nonsense
            throw new IllegalArgumentException("segmentBitShift must be between 0 and " + Long.SIZE);
        }

        this.rolledSegments = loadRolledSegments(directory, state);
        this.currentSegment = loadCurrentSegment(state);

        logger.info("SEGMENT BIT SHIFT: {}", metadata.segmentBitShift);
        logger.info("MAX SEGMENTS: {} ({} bits)", maxSegments, Long.SIZE - metadata.segmentBitShift);
        logger.info("MAX ADDRESS PER SEGMENT: {} ({} bits)", maxAddressPerSegment, metadata.segmentBitShift);

        this.scheduler.scheduleAtFixedRate(() -> LogFileUtils.writeState(directory, state), 5, 1, TimeUnit.SECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            this.flushInternal();
            this.close();
        }));
    }

    public static <T> Builder<T> builder(File directory, Serializer<T> serializer) {
        return new Builder<>(directory, serializer);
    }

    protected abstract L createSegment(Storage storage, Serializer<T> serializer, DataReader reader);

    protected abstract L openSegment(Storage storage, Serializer<T> serializer, DataReader reader, long position, boolean readonly);

    private L createSegmentInternal(String name) {
        File segmentFile = LogFileUtils.newSegmentFile(directory, name, segmentsNames());
        Storage storage = createStorage(segmentFile, metadata.segmentSize);

        return createSegment(storage, serializer, dataReader);
    }

    private L loadCurrentSegment(State state) {
        if (state.currentSegment == null) {
            logger.info("No rolledSegments available creating");

            L segment = createSegmentInternal(nextSegmentName());
            this.state.currentSegment = segment.name();
            LogFileUtils.writeState(directory, this.state);
            return segment;
        }

        File segmentFile = LogFileUtils.loadSegment(directory, state.currentSegment);
        L segment = loadSegment(segmentFile, state.position, false);

        long logPos = segment.checkIntegrity(state.position);
        this.state.position = toSegmentedPosition(rolledSegments.size(), logPos);
        this.state.currentSegment = segment.name();

        LogFileUtils.writeState(directory, this.state);
        return segment;
    }

    private String nextSegmentName() {
        List<String> allSegmentNames = withCurrentSegment().stream().map(Log::name).collect(Collectors.toList());
        return namingStrategy.name(allSegmentNames);
    }

    private LinkedList<L> loadRolledSegments(final File directory, final State state) {
        LinkedList<L> segments = new LinkedList<>();
        for (String rolledSegment : state.rolledSegments) {
            File segmentFile = LogFileUtils.loadSegment(directory, rolledSegment);
            L segment = loadSegment(segmentFile, 0, true);
            segments.add(segment);
        }

        return segments;
    }

    private L loadSegment(File segmentFile, long position, boolean readonly) {
        Storage storage = openStorage(segmentFile);
        return openSegment(storage, serializer, dataReader, position, readonly);
    }

    private Storage openStorage(File file) {
        if (mmap)
            return new MMapStorage(file, file.length(), FileChannel.MapMode.READ_WRITE);
        return new RafStorage(file, file.length());
    }

    private LinkedList<L> withCurrentSegment() {
        LinkedList<L> allSegments = new LinkedList<>(rolledSegments);
        if (currentSegment != null) {
            allSegments.add(currentSegment);
        }
        return allSegments;
    }

    private Storage createStorage(File file, long length) {
        //extra length for the last entry, to avoid remapping
        length = (long) (length + (length * SEGMENT_EXTRA_SIZE));
        if (mmap)
            return new MMapStorage(file, length, FileChannel.MapMode.READ_WRITE);
        return new RafStorage(file, length);
    }

    public void roll() {
        try {
            logger.info("Rolling appender");
            flush();

            L newSegment = createSegmentInternal(nextSegmentName());
            currentSegment.roll();

            rolledSegments.add(currentSegment);
            state.rolledSegments.add(currentSegment.name());

            currentSegment = newSegment;
            state.currentSegment = newSegment.name();

            state.lastRollTime = System.currentTimeMillis();
            LogFileUtils.writeState(directory, state);

        } catch (Exception e) {
            throw new RuntimeIOException("Could not close segment file", e);
        }
    }


    int getSegment(long position) {
        long segmentIdx = (position >>> metadata.segmentBitShift);
        if (segmentIdx > maxSegments) {
            throw new IllegalArgumentException("Invalid segment, value cannot be greater than " + maxSegments);
        }
        if (segmentIdx > rolledSegments.size()) {
            throw new IllegalArgumentException("No segment for address " + position + " (segmentIdx: " + segmentIdx + "), available rolledSegments: " + rolledSegments.size());
        }
        return (int) segmentIdx;
    }

    long toSegmentedPosition(long segmentIdx, long position) {
        if (segmentIdx < 0) {
            throw new IllegalArgumentException("Segment index must be greater than zero");
        }
        if (segmentIdx > maxSegments) {
            throw new IllegalArgumentException("Segment index cannot be greater than " + maxSegments);
        }
        return (segmentIdx << metadata.segmentBitShift) | position;
    }

    long getPositionOnSegment(long position) {
        long mask = (1L << metadata.segmentBitShift) - 1;
        return (position & mask);
    }

    private boolean shouldRoll(L currentSegment) {
        return currentSegment.size() > metadata.segmentSize && currentSegment.size() > 0;
    }

    public void merge(String newSegmentName, SegmentCombiner<T> combiner, int from, int to) {

        List<String> segments = segmentsNames().subList(from, to + 1);

        List<L> foundSegments = new ArrayList<>();
        long totalSize = 0;
        for (String segmentName : segments) {
            if (segmentName.equals(currentSegment.name())) {
                throw new IllegalStateException("Cannot merge segment " + currentSegment.name() + " as it still opened for writes");
            }
            L log = this.rolledSegments.stream().filter(l -> l.name().equals(segmentName)).findFirst().orElseThrow(() -> new IllegalArgumentException("No segment found: " + segmentName));

            foundSegments.add(log);
            totalSize += log.size();
        }

        if (totalSize > maxAddressPerSegment) {
            //TODO better approach than just throwing an exception ?
            throw new IllegalStateException("New segment position " + totalSize + " exceeds the maximum segment position: " + maxAddressPerSegment);
        }

        File newSegmentFile = new File(directory, newSegmentName);
        L newSegment = null;
        try {
            Storage storage = createStorage(newSegmentFile, totalSize);
            newSegment = createSegment(storage, serializer, dataReader);

            combiner.merge(foundSegments.stream().map(Log::stream).collect(Collectors.toList()), newSegment::append);

            //TODO add 'tobeRemoved' list to be removed once cleared up
            //TODO investigate level and position tiered merges

            //TODO update segment position
            newSegment.roll();


            List<L> newSegmentOrder = new LinkedList<>();
            for (int i = 0; i < from; i++) {
                newSegmentOrder.add(rolledSegments.get(i));
            }
            newSegmentOrder.add(newSegment);

            for (int i = to + 1; i <= rolledSegments.size() - 1; i++) {
                newSegmentOrder.add(rolledSegments.get(i));
            }
            //delete the merged segmentsNames
            foundSegments.stream().map(Log::name).forEach(this::delete);
            this.rolledSegments.clear();
            this.rolledSegments.addAll(newSegmentOrder);


        } catch (Exception e) {
            IOUtils.closeQuietly(newSegment);
            try {
                Files.delete(newSegmentFile.toPath());
            } catch (IOException e1) {
                logger.error("Failed to delete merge result segment " + newSegmentName, e1);
            }
            throw e;
        }
    }


    public long append(T data) {
        long positionOnSegment = currentSegment.append(data);
        long segmentedPosition = toSegmentedPosition(rolledSegments.size(), positionOnSegment);
        if (positionOnSegment < 0) {
            throw new IllegalStateException("Invalid address " + positionOnSegment);
        }
        if (shouldRoll(currentSegment)) {
            roll();
        }
        state.position = currentSegment.position();
        state.entryCount++;
        return segmentedPosition;
    }

    public String name() {
        return directory.getName();
    }

    public Scanner<T> scanner() {
        return new RollingSegmentReader(withCurrentSegment(), 0);
    }

    public Stream<T> stream() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(scanner(), Spliterator.ORDERED), false);
    }

    public Scanner<T> scanner(long position) {
        return new RollingSegmentReader(new LinkedList<>(withCurrentSegment()), position);
    }

    public long position() {
        return state.position;
    }

    public T get(long position) {
        int segmentIdx = getSegment(position);
        if (segmentIdx < 0) {
            return null;
        }
        long positionOnSegment = getPositionOnSegment(position);
        L segment = segmentIdx == rolledSegments.size() ? currentSegment : rolledSegments.get(segmentIdx);
        if (segment != null) {
            return segment.get(positionOnSegment);
        }
        return null;
    }

    public long size() {
        return rolledSegments.stream().mapToLong(Log::size).sum() + currentSegment.size();
    }

    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        logger.info("Closing log appender {}", directory.getName());
        scheduler.shutdown();

        if (currentSegment != null) {
            IOUtils.flush(currentSegment);
            state.position = currentSegment.position();
            logger.info("Writing state {}", state);
            LogFileUtils.writeState(directory, state);

            logger.info("Closing segment {} (current)", currentSegment.name());
            IOUtils.closeQuietly(currentSegment);
        }

        for (L segment : rolledSegments) {
            logger.info("Closing segment {}", segment.name());
            IOUtils.closeQuietly(segment);
        }
    }

    public void flush() {
        logger.info("Flushing");
        if (metadata.asyncFlush)
            executor.execute(this::flushInternal);
        else
            this.flushInternal();

    }

    private void flushInternal() {
        try {
            long start = System.currentTimeMillis();
            currentSegment.flush();
            logger.info("Flush took {}ms", System.currentTimeMillis() - start);
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    public List<String> segmentsNames() {
        List<String> names = rolledSegments.stream().map(Log::name).collect(Collectors.toList());
        if (currentSegment != null) {
            names.add(currentSegment.name());
        }
        return names;
    }

    public void add(L segment) {
        IOUtils.flush(segment);
        this.rolledSegments.addFirst(segment);
        this.state.rolledSegments.addFirst(segment.name());
        LogFileUtils.writeState(directory, this.state);
    }

    public long entries() {
        return this.state.entryCount;
    }

    public String currentSegment() {
        return currentSegment.name();
    }

    public L current() {
        return currentSegment;
    }

    public Iterator<L> segments() {
        return withCurrentSegment().iterator();
    }

    public Iterator<L> segmentsReverse() {
        return withCurrentSegment().descendingIterator();
    }

    public Path directory() {
        return directory.toPath();
    }

    public void delete(String segment) {
        if (currentSegment.name().equals(segment)) {
            throw new IllegalStateException("Cannot delete current writing segment");
        }

        Optional<L> found = rolledSegments.stream().filter(l -> l.name().equals(segment)).findFirst();
        L log = found.orElseThrow(() -> new IllegalArgumentException("Segment not found for name " + segment));

        state.rolledSegments.remove(segment);
        LogFileUtils.writeState(directory, state);
        rolledSegments.remove(log);
        log.delete();
    }

    private class RollingSegmentReader extends Scanner<T> {

        private final List<L> segments;
        private Scanner<T> current;
        private int segmentIdx;

        RollingSegmentReader(List<L> segments, long position) {
            super(null, null, null, position);
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
