package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.DataReader;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.seda.SedaContext;
import io.joshworks.fstore.core.seda.Stage;
import io.joshworks.fstore.core.seda.StageHandler;
import io.joshworks.fstore.core.seda.StageStats;
import io.joshworks.fstore.log.BitUtil;
import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.LogFileUtils;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.PollingSubscriber;
import io.joshworks.fstore.log.appender.compaction.Compactor;
import io.joshworks.fstore.log.appender.level.Levels;
import io.joshworks.fstore.log.appender.naming.NamingStrategy;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Position address schema
 * <p>
 * |------------ 64bits -------------|
 * [SEGMENT_IDX] [POSITION_ON_SEGMENT]
 */
public abstract class LogAppender<T, L extends Log<T>> implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(LogAppender.class);

    private final File directory;
    private final Serializer<T> serializer;
    private final Metadata metadata;
    private final DataReader dataReader;
    private final NamingStrategy namingStrategy;
    private final SegmentFactory<T, L> factory;
    private final StorageProvider storageProvider;

    final long maxSegments;
    final long maxAddressPerSegment;

    //LEVEL0 [CURRENT_SEGMENT]
    //LEVEL1 [SEG1][SEG2]
    //LEVEL2 [SEG3][SEG4]
    //LEVEL3 ...
    final Levels<T, L> levels;

    //state
    private final State state;
    private final boolean compactionDisabled;
//    private final History history;

    private AtomicBoolean closed = new AtomicBoolean();

    //    private final ScheduledExecutorService stateScheduler = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService executor = Executors.newFixedThreadPool(3);
    private final SedaContext sedaContext = new SedaContext();
    private final Set<LogPoller> pollers = new HashSet<>();

    private final Compactor<T, L> compactor;

    protected LogAppender(Config<T> config, SegmentFactory<T, L> factory) {

        this.directory = config.directory;
        this.serializer = config.serializer;
        this.factory = factory;
        this.storageProvider = config.mmap ? StorageProvider.mmap(config.mmapBufferSize) : StorageProvider.raf();
        this.dataReader = config.reader;
        this.namingStrategy = config.namingStrategy;

        boolean metadataExists = LogFileUtils.metadataExists(directory);

        if (!metadataExists) {
            logger.info("Creating LogAppender");

            LogFileUtils.createRoot(directory);
            this.metadata = Metadata.create(
                    directory,
                    config.segmentSize,
                    config.segmentBitShift,
                    config.maxSegmentsPerLevel,
                    config.mmap,
                    config.flushAfterWrite,
                    config.asyncFlush);

            this.state = State.empty(directory);
        } else {
            logger.info("Opening LogAppender");
            this.metadata = Metadata.readFrom(directory);
            this.state = State.readFrom(directory);
        }

//        this.history = new History(directory);

        this.maxSegments = BitUtil.maxValueForBits(Long.SIZE - metadata.segmentBitShift);
        this.maxAddressPerSegment = BitUtil.maxValueForBits(metadata.segmentBitShift);

        if (metadata.segmentBitShift >= Long.SIZE || metadata.segmentBitShift < 0) {
            //just a numeric validation, values near 64 and 0 are still nonsense
            IOUtils.closeQuietly(state);
            throw new IllegalArgumentException("segmentBitShift must be between 0 and " + Long.SIZE);
        }

        try {
            this.levels = loadSegments();
        } catch (Exception e) {
            IOUtils.closeQuietly(state);
            throw e;
        }

        this.compactionDisabled = config.compactionDisabled;
        logger.info("Compaction is {}", this.compactionDisabled ? "DISABLED" : "ENABLED");

        this.compactor = new Compactor<>(directory, config.combiner, factory, storageProvider, serializer, dataReader, namingStrategy, metadata.maxSegmentsPerLevel, metadata.magic, levels, sedaContext, config.threadPerLevel);


        logger.info("SEGMENT BIT SHIFT: {}", metadata.segmentBitShift);
        logger.info("MAX SEGMENTS: {} ({} bits)", maxSegments, Long.SIZE - metadata.segmentBitShift);
        logger.info("MAX ADDRESS PER SEGMENT: {} ({} bits)", maxAddressPerSegment, metadata.segmentBitShift);

//        this.stateScheduler.scheduleAtFixedRate(() -> state.flush(), 5, 1, TimeUnit.SECONDS);
//
//        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//            this.flushInternal();
//            this.close();
//        }));


        sedaContext.addStage("write", (StageHandler<WriteItem>) elem -> appendAsyncInternal(elem.data), new Stage.Builder());

    }

    public static <T> Config<T> builder(File directory, Serializer<T> serializer) {
        return new Config<>(directory, serializer);
    }

    private L createCurrentSegment(long size) {
        File segmentFile = LogFileUtils.newSegmentFile(directory, namingStrategy, 1);
        Storage storage = storageProvider.create(segmentFile, size + (size / 10));

        return factory.createOrOpen(storage, serializer, dataReader, metadata.magic, Type.LOG_HEAD);
    }

    private Levels<T, L> loadSegments() {

        List<L> segments = new ArrayList<>();
        try {
            for (String segmentName : LogFileUtils.findSegments(directory)) {
                segments.add(loadSegment(segmentName));
            }

            long levelZeroSegments = segments.stream().filter(l -> l.level() == 0).count();

            if (levelZeroSegments == 0) {
                //create current segment
                L currentSegment = createCurrentSegment(metadata.segmentSize);
                segments.add(currentSegment);
            }
            if (levelZeroSegments > 1) {
                throw new IllegalStateException("TODO - Multiple level zero segments");
            }

        } catch (Exception e) {
            segments.forEach(IOUtils::closeQuietly);
            throw e;
        }


        return Levels.create(metadata.maxSegmentsPerLevel, segments);
    }

    private L loadSegment(String segmentName) {
        Storage storage = null;
        try {
            File segmentFile = LogFileUtils.getSegmentHandler(directory, segmentName);
            storage = storageProvider.open(segmentFile);
            L segment = factory.createOrOpen(storage, serializer, dataReader, metadata.magic, null);
            logger.info("Loaded segment {}", segment);
            return segment;
        } catch (Exception e) {
            IOUtils.closeQuietly(storage);
            throw e;
        }
    }

    public Map<String, StageStats> stats() {
        return sedaContext.stats();
    }

    public synchronized void roll() {
        try {

            logger.info("Rolling appender");
            flush();

            L newSegment = createCurrentSegment(metadata.segmentSize);
            levels.appendSegment(newSegment);

            addToPollers(newSegment);

            state.lastRollTime(System.currentTimeMillis());
            state.flush();

            if (!compactionDisabled) {
                compactor.requestCompaction(1);
            }


        } catch (Exception e) {
            throw new RuntimeIOException("Could not roll segment file", e);
        }
    }

    int getSegment(long position) {
        long segmentIdx = (position >>> metadata.segmentBitShift);
        if (segmentIdx > maxSegments) {
            throw new IllegalArgumentException("Invalid segment, value cannot be greater than " + maxSegments);
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
        long segmentSize = currentSegment.size();
        return segmentSize >= metadata.segmentSize && segmentSize > 0;
    }

    public void compact() {
        compactor.forceCompaction(1);
    }

    public void appendAsync(T data, Consumer<Long> onComplete) {
        sedaContext.submit("write", new WriteItem(data, onComplete));
    }

    private void appendAsyncInternal(WriteItem item) {
        long position = this.append(item.data);
        item.completionHandler.accept(position);
    }

    private void addToPollers(L newSegment) {
        for (LogPoller poller : pollers) {
            poller.addSegment(newSegment);
        }
    }

    private class WriteItem {
        private final T data;
        private final Consumer<Long> completionHandler;

        private WriteItem(T data, Consumer<Long> completionHandler) {
            this.data = data;
            this.completionHandler = completionHandler;
        }
    }

    public long append(T data) {
        L current = levels.current();
        if (shouldRoll(current)) {
            roll();
            current = levels.current();
        }
        long positionOnSegment = current.append(data);
        if (metadata.flushAfterWrite) {
            flushInternal();
        }
        long segmentedPosition = toSegmentedPosition(levels.numSegments() - 1L, positionOnSegment);
        if (positionOnSegment < 0) {
            throw new IllegalStateException("Invalid address " + positionOnSegment);
        }

        state.position(current.position());
        state.incrementEntryCount();
        return segmentedPosition;
    }

    public String name() {
        return directory.getName();
    }

    //TODO implement reader pool, instead using a new instance of reader, provide a pool of reader to better performance
    public LogIterator<T> scanner() {
        return new RollingSegmentReader(Log.START);
    }

    public Stream<T> stream() {
        return Iterators.stream(scanner());
    }

    public LogIterator<T> scanner(long position) {
        return new RollingSegmentReader(position);
    }

    public PollingSubscriber<T> poller() {
        return createPoller(Log.START);
    }

    public PollingSubscriber<T> poller(long position) {
        return createPoller(position);
    }

    private PollingSubscriber<T> createPoller(long position) {
        LogPoller logPoller = new LogPoller(position);
        pollers.add(logPoller);
        return logPoller;
    }


    public long position() {
        return state.position();
    }

    public T get(long position) {
        int segmentIdx = getSegment(position);
        validateSegmentIdx(segmentIdx, position);

        long positionOnSegment = getPositionOnSegment(position);
        L segment = levels.get(segmentIdx);
        if (segment != null) {
            return segment.get(positionOnSegment);
        }
        return null;
    }

    void validateSegmentIdx(int segmentIdx, long pos) {
        if (segmentIdx < 0 || segmentIdx > levels.numSegments()) {
            throw new IllegalArgumentException("No segment for address " + pos + " (segmentIdx: " + segmentIdx + "), available segments: " + levels.numSegments());
        }
    }

    public long size() {
        return Iterators.stream(segments(Order.BACKWARD)).mapToLong(Log::size).sum();
    }

    public Stream<L> streamSegments(Order order) {
        return Iterators.stream(segments(order));
    }

    public long size(int level) {
        return segments(level).stream().mapToLong(Log::size).sum();
    }

    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        logger.info("Closing log appender {}", directory.getName());
//        stateScheduler.shutdown();

        sedaContext.shutdown();

        L currentSegment = levels.current();
        if (currentSegment != null) {
            IOUtils.flush(currentSegment);
            state.position(currentSegment.position());
        }

        for (LogPoller poller : pollers) {
            poller.close();
        }

        state.flush();
        state.close();

        streamSegments(Order.FORWARD).forEach(segment -> {
            logger.info("Closing segment {}", segment.name());
            IOUtils.closeQuietly(segment);
        });

    }

    public void flush() {
        if (closed.get()) {
            return;
        }
        if (metadata.asyncFlush)
            executor.execute(this::flushInternal);
        else
            this.flushInternal();

    }

    private void flushInternal() {
        if (closed.get()) {
            return;
        }
        try {
//            long start = System.currentTimeMillis();
            levels.current().flush();
//            logger.info("Flush took {}ms", System.currentTimeMillis() - start);
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    public List<String> segmentsNames() {
        return streamSegments(Order.FORWARD).map(Log::name).collect(Collectors.toList());
    }

    public long entries() {
        return this.state.entryCount();
    }

    public String currentSegment() {
        return levels.current().name();
    }

    L current() {
        return levels.current();
    }

    public LogIterator<L> segments(Order order) {
        return levels.segments(order);
    }

    private List<L> segments(int level) {
        if (level < 0) {
            throw new IllegalArgumentException("Level must be at least zero");
        }
        if (level > levels.depth()) {
            throw new IllegalArgumentException("No such level " + level + ", current depth: " + levels.depth());
        }
        return levels.segments(level);
    }

    public int depth() {
        return levels.depth();
    }

    public Path directory() {
        return directory.toPath();
    }

    /**
     * This Reader only works when using OLDEST ordering.
     */
    private class RollingSegmentReader implements LogIterator<T> {

        private final Iterator<LogIterator<T>> segmentsIterators;
        private LogIterator<T> current;
        private int segmentIdx;

        RollingSegmentReader(long startPosition) {
            Iterator<L> segments = segments(Order.FORWARD);
            this.segmentIdx = getSegment(startPosition);
            validateSegmentIdx(segmentIdx, startPosition);
            long positionOnSegment = getPositionOnSegment(startPosition);

            // skip
            for (int i = 0; i <= segmentIdx - 1; i++) {
                segments.next();
            }

            if (segments.hasNext()) {
                this.current = segments.next().iterator(positionOnSegment);
            }

            List<LogIterator<T>> subsequentIterators = new ArrayList<>();
            while (segments.hasNext()) {
                subsequentIterators.add(segments.next().iterator());
            }
            this.segmentsIterators = subsequentIterators.iterator();

        }

        @Override
        public long position() {
            return toSegmentedPosition(segmentIdx, current.position());
        }

        @Override
        public boolean hasNext() {
            if (current == null) {
                return false;
            }
            boolean hasNext = current.hasNext();
            if (!hasNext) {
                if (!segmentsIterators.hasNext()) {
                    return false;
                }
                current = segmentsIterators.next();
                segmentIdx++; //TODO verify if the segment index is correct, also how iterating during the merge ?
                return current.hasNext();
            }
            return true;
        }

        @Override
        public T next() {
            return current.next();
        }

        @Override
        public void close() {
            try {
                current.close();
            } catch (IOException e) {
                throw RuntimeIOException.of(e);
            }
        }
    }

    private class LogPoller implements PollingSubscriber<T> {

        private final BlockingQueue<PollingSubscriber<T>> segmentQueue = new LinkedBlockingQueue<>();
        private final int MAX_SEGMENT_WAIT_SEC = 5;
        private PollingSubscriber<T> currentPoller;
        private int segmentIdx;
        private final AtomicBoolean closed = new AtomicBoolean();

        LogPoller(long startPosition) {
            this.segmentIdx = getSegment(startPosition);
            validateSegmentIdx(segmentIdx, startPosition);
            Iterator<L> segments = segments(Order.FORWARD);
            long positionOnSegment = getPositionOnSegment(startPosition);

            // skip
            for (int i = 0; i <= segmentIdx - 1; i++) {
                segments.next();
            }

            if (segments.hasNext()) {
                this.currentPoller = segments.next().poller(positionOnSegment);
            }

            while (segments.hasNext()) {
                segmentQueue.add(segments.next().poller());
            }

        }

        @Override
        public T peek() throws InterruptedException {
            return peekData();
        }

        @Override
        public T poll() throws InterruptedException {
            return pollData(PollingSubscriber.NO_SLEEP, TimeUnit.SECONDS);
        }

        @Override
        public T poll(long limit, TimeUnit timeUnit) throws InterruptedException {
            return pollData(limit, timeUnit);
        }

        @Override
        public T take() throws InterruptedException {
            return takeData();
        }

        private synchronized T pollData(long limit, TimeUnit timeUnit) throws InterruptedException {
            if (closed.get()) {
                return null;
            }
            T item = currentPoller.poll(limit, timeUnit);
            if (nextSegment() && item == null)
                return pollData(limit, timeUnit);
            return item;
        }

        private boolean nextSegment() throws InterruptedException {
            if (currentPoller.headOfLog()) { //end of segment
                closePoller(this.currentPoller);
                this.currentPoller = waitForNextSegment();
                if (this.currentPoller == null) { //close was called
                    return false;
                }
                segmentIdx++;
                return true;
            }
            return false;
        }

        private synchronized T peekData() throws InterruptedException {
            if (closed.get()) {
                return null;
            }
            T item = currentPoller.peek();
            if (nextSegment() && item == null)
                return peek();
            return item;
        }

        private synchronized T takeData() throws InterruptedException {
            if (closed.get()) {
                return null;
            }
            T item = currentPoller.take();
            if (currentPoller.headOfLog()) { //end of segment
                closePoller(this.currentPoller);
                this.currentPoller = waitForNextSegment();

                if (this.currentPoller == null) { //close was called
                    return item;
                }
                segmentIdx++;
                if (item != null) {
                    return item;
                }
                return takeData();
            }
            return item;
        }

        private void closePoller(PollingSubscriber<T> currentPoller) {
            try {
                currentPoller.close();
            } catch (IOException e) {
                throw RuntimeIOException.of(e);
            }
        }

        private PollingSubscriber<T> waitForNextSegment() throws InterruptedException {
            PollingSubscriber<T> next = null;
            while (!closed.get() && next == null) {
                next = segmentQueue.poll(MAX_SEGMENT_WAIT_SEC, TimeUnit.SECONDS);//wait next segment, should never wait really
            }
            return next;
        }

        @Override
        public boolean headOfLog() {
            return segmentIdx == levels.numSegments() && currentPoller.headOfLog();
        }

        @Override
        public long position() {
            return toSegmentedPosition(segmentIdx, currentPoller.position());
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            try {
                LogAppender.this.pollers.remove(this);
                currentPoller.close();
            } catch (IOException e) {
                throw RuntimeIOException.of(e);
            }
        }

        private void addSegment(L segment) {
            segmentQueue.add(segment.poller());
        }

    }

}
