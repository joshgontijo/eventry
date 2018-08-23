package io.joshworks.fstore.es.index;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.es.index.disk.IndexAppender;
import io.joshworks.fstore.es.index.disk.IndexCompactor;
import io.joshworks.fstore.es.index.disk.IndexEntrySerializer;
import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.PollingSubscriber;
import io.joshworks.fstore.log.appender.LogAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Flushable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class TableIndex implements Index, Flushable {

    private static final Logger logger = LoggerFactory.getLogger(TableIndex.class);
    public static final int DEFAULT_FLUSH_THRESHOLD = 1000000;
    public static final boolean DEFAULT_USE_COMPRESSION = true;
    private static final String INDEX_DIR = "index";
    private static final String INDEX_WRITER = "index-writer";
    private final int flushThreshold; //TODO externalize

    //    private final EventLog log;
    private final IndexAppender diskIndex;
    private MemIndex memIndex = new MemIndex();

//    private final SedaContext sedaContext = new SedaContext();

    public TableIndex(File rootDirectory) {
        this(rootDirectory, DEFAULT_FLUSH_THRESHOLD, DEFAULT_USE_COMPRESSION);
    }

    public TableIndex(File rootDirectory, int flushThreshold, boolean useCompression) {
//        this.log = log;
        if (flushThreshold < 1000) {//arbitrary number
            throw new IllegalArgumentException("Flush threshold must be at least 1000");
        }
        diskIndex = new IndexAppender(LogAppender
                .builder(new File(rootDirectory, INDEX_DIR), new IndexEntrySerializer())
                .compactionStrategy(new IndexCompactor())
                .maxSegmentsPerLevel(2)
                .segmentSize(flushThreshold * IndexEntry.BYTES)
                .namingStrategy(new IndexAppender.IndexNaming()), flushThreshold, useCompression);

        this.flushThreshold = flushThreshold;
//        this.sedaContext.addStage(INDEX_WRITER, this::writeToDiskAsync, new Stage.Builder().corePoolSize(1).maximumPoolSize(1).blockWhenFull().queueSize(10));
    }

    public IndexEntry add(long stream, int version, long position) {
        if (version <= IndexEntry.NO_VERSION) {
            throw new IllegalArgumentException("Version must be greater than or equals to zero");
        }
        if (position < 0) {
            throw new IllegalArgumentException("Position must be greater than zero");
        }
        IndexEntry entry = IndexEntry.of(stream, version, position);
        memIndex.add(entry);
        if (memIndex.size() >= flushThreshold) {
            writeToDisk();
            memIndex = new MemIndex();
        }
        return entry;
    }

//    private void writeToDiskAsync(final EventContext<Set<IndexEntry>> ctx) {
//        writeToDisk(ctx.data);
//    }

    private void writeToDisk() {
        logger.info("Writing index to disk");
        if (memIndex.isEmpty()) {
            return;
        }
        for (IndexEntry indexEntry : memIndex) {
            diskIndex.append(indexEntry);
        }
        diskIndex.roll();
//        long checkpoint = log.position();
//        writeCheckpoint(checkpoint);
    }

    @Override
    public int version(long stream) {
        int version = memIndex.version(stream);
        if (version > IndexEntry.NO_VERSION) {
            return version;
        }

        return diskIndex.version(stream);
    }

    public long size() {
        return diskIndex.entries() + memIndex.size();
    }

    @Override
    public void close() {
        this.flush();
        memIndex.close();
        diskIndex.close();
    }

    @Override
    public Stream<IndexEntry> stream(Range range) {
        return Iterators.stream(iterator(range));
    }

    @Override
    public Stream<IndexEntry> stream() {
        return Iterators.stream(iterator());
    }

    @Override
    public LogIterator<IndexEntry> iterator(Range range) {
        LogIterator<IndexEntry> cacheIterator = memIndex.iterator(range);
        LogIterator<IndexEntry> diskIterator = diskIndex.iterator(range);

        return joiningDiskAndMem(diskIterator, cacheIterator);
    }

    @Override
    public Optional<IndexEntry> get(long stream, int version) {
        Optional<IndexEntry> fromMemory = memIndex.get(stream, version);
        if (fromMemory.isPresent()) {
            return fromMemory;
        }
        return diskIndex.get(stream, version);
    }

    @Override
    public LogIterator<IndexEntry> iterator() {
        return joiningDiskAndMem(diskIndex.iterator(), memIndex.iterator());
    }

    private LogIterator<IndexEntry> joiningDiskAndMem(LogIterator<IndexEntry> diskIterator, LogIterator<IndexEntry> memIndex) {
        return Iterators.concat(Arrays.asList(diskIterator, memIndex));
    }

    @Override
    public void flush() {
        writeToDisk();
        memIndex = new MemIndex();
    }

    public PollingSubscriber<IndexEntry> poller(long stream, int version) {

        Optional<IndexEntry> fromMemory = memIndex.get(stream, version);

        //TODO if present 'fromMemory' was the latest, the next item should be used instead
        //TODO pass the current index
        if (fromMemory.isPresent()) {
            //TODO the disk position should be the last position in the log
            return new IndexPoller(diskIndex.poller(fromMemory.get().position), true);
        } else if (diskIndex.entries() == 0) {
            return new IndexPoller(diskIndex.poller(), true);
        } else {
            return new IndexPoller(diskIndex.poller(), false);
        }
    }


    private class IndexPoller implements PollingSubscriber<IndexEntry> {

        private final PollingSubscriber<IndexEntry> diskPoller;
        private PollingSubscriber<IndexEntry> memPoller;
        private volatile boolean memPolling;
        private volatile boolean flushedIndex;
        private long readFromMemory;

        private IndexPoller(PollingSubscriber<IndexEntry> diskPoller, boolean memPolling) {
            this.diskPoller = diskPoller;
            this.memPolling = memPolling;
            this.memPoller = memIndex.poller();
        }

        private PollingSubscriber<IndexEntry> getPoller() {
            return memPolling ? memPoller : diskPoller;
        }

        @Override
        public synchronized IndexEntry peek() throws InterruptedException {
            return null;
        }

        @Override
        public synchronized IndexEntry poll() throws InterruptedException {
            if(flushedIndex && memPolling) {
                memPolling = false;
                memPoller = memIndex.poller();
                skipReadEntries();
            }
            var poller = getPoller();
            var indexEntry = poller.poll();
            if (indexEntry == null && !memPolling) { //no more data on this log
                //done processing disk data switch to the memory
                    memPolling = true;
                    memPoller = memIndex.poller();
                    return poll();

//                //done processing last mem poller, and we've got a new one, switch to the new one, unless there's a disk poller
//                if (memPolling && memPoller.headOfLog()) {
//
//                    memPoller = memIndex.poller();
//                    return poll();
//                }

            }
            if(memPolling && indexEntry != null) {
                readFromMemory++;
            }
            return indexEntry;

        }

        private void skipReadEntries() throws InterruptedException {
            long skipped = 0;
            while (skipped < readFromMemory) {
                IndexEntry polled = diskPoller.poll();
                if(polled != null) {
                    skipped++;
                }else {
                    logger.warn("Expected data to be available on disk");
                }
            }
        }

        @Override
        public synchronized IndexEntry poll(long limit, TimeUnit timeUnit) throws InterruptedException {
            return null;
        }

        @Override
        public synchronized IndexEntry take() throws InterruptedException {
            return null;
        }

        @Override
        public boolean headOfLog() {
            return false;
        }

        @Override
        public long position() {
            return 0;
        }

        @Override
        public  void close() throws IOException {

        }

        public synchronized void onFlush() {
            if(memPolling) {
                memPolling = false;
            }
            try {
                memPoller.close();
            } catch (IOException e) {
                throw RuntimeIOException.of(e);
            }
        }

    }


    //TODO LogAppender's LOG_HEAD segment must be able to store different data layout: with stream name in this case
//    private MemIndex restoreFromCheckpoint(){
//        long checkpoint = readCheckpoint();
//
//        MemIndex memIndex = new MemIndex();
//        if(checkpoint > 0) {
//            return memIndex;
//        }
//
//        try(LogIterator<Event> scanner = log.scanner(checkpoint)) {
//            while(scanner.hasNext()) {
//                long position = scanner.position();
//                Event next = scanner.next();
//                String stream = next.stream();
//                int version = version(next.version());
//
//                memIndex.add(IndexEntry.of(stream, version, position));
//
//            }
//        } catch (IOException e) {
//            throw new IllegalStateException("Failed to restore index from checkpoint at position " + checkpoint, e);
//        }
//
//
//    }
//
//    private long readCheckpoint() {
//        logger.info("Loading index checkpoint");
//        try(Storage storage = new RafStorage(new File(directory, ".checkpoint"), 1024, Mode.READ_WRITE)) {
//
//            ByteBuffer bb = ByteBuffer.allocate(1024);
//            storage.read(0, bb);
//            bb.flip();
//            if(bb.hasRemaining()) {
//                return  bb.getLong();
//            }
//            return 0;
//        } catch (IOException e) {
//            throw RuntimeIOException.of(e);
//        }
//    }
//
//    private void writeCheckpoint(long checkpoint) {
//        logger.info("Updating index checkpoint");
//        try(Storage storage = new RafStorage(new File(directory, ".checkpoint"), 1024, Mode.READ_WRITE)) {
//
//            ByteBuffer bb = ByteBuffer.allocate(1024);
//            bb.putLong(checkpoint);
//            bb.flip();
//            storage.write(bb);
//        } catch (IOException e) {
//            throw RuntimeIOException.of(e);
//        }
//    }
}