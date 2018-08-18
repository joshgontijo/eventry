package io.joshworks.fstore.es.index;

import io.joshworks.fstore.es.index.disk.IndexAppender;
import io.joshworks.fstore.es.index.disk.IndexCompactor;
import io.joshworks.fstore.es.index.disk.IndexEntrySerializer;
import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.LogAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Flushable;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;

public class TableIndex implements Index, Flushable {

    private static final Logger logger = LoggerFactory.getLogger(TableIndex.class);
    public static final int DEFAULT_FLUSH_THRESHOLD = 1000000;
    public static final boolean DEFAULT_USE_COMPRESSION = true;
    private static final String INDEX_DIR = "index";
    private final int flushThreshold; //TODO externalize

//    private final EventLog log;
    private final IndexAppender diskIndex;
    private MemIndex memIndex = new MemIndex();

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
        }
        return entry;
    }

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
        memIndex = new MemIndex();
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