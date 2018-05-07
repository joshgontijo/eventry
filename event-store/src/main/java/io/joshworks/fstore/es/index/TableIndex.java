package io.joshworks.fstore.es.index;

import io.joshworks.fstore.es.index.disk.IndexAppender;
import io.joshworks.fstore.es.index.disk.IndexEntrySerializer;
import io.joshworks.fstore.es.utils.Iterators;
import io.joshworks.fstore.log.appender.LogAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Flushable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class TableIndex implements Index, Flushable {

    private static final Logger logger = LoggerFactory.getLogger(TableIndex.class);
    public static final int DEFAULT_FLUSH_THRESHOLD = 500000;
    private static final String INDEX_DIR = "index";
    private final int flushThreshold;

    private MemIndex memIndex = new MemIndex();
    private final IndexAppender diskIndex;

    public TableIndex(File rootDirectory) {
        this(rootDirectory, DEFAULT_FLUSH_THRESHOLD);
    }

    public TableIndex(File rootDirectory, int flushThreshold) {
        if(flushThreshold < 1000) {//arbitrary number
            throw new IllegalArgumentException("Flush threshold must be at least 1000");
        }
        diskIndex = new IndexAppender(LogAppender
                .builder(new File(rootDirectory, INDEX_DIR), new IndexEntrySerializer())
                .namingStrategy(new IndexAppender.IndexNaming())
                .mmap(), flushThreshold);

        this.flushThreshold = flushThreshold;
    }

    public void add(long stream, int version, long position) {
        if (version <= 0) {
            throw new IllegalArgumentException("Version must be greater than zero");
        }
        if (position < 0) {
            throw new IllegalArgumentException("Position must be greater than zero");
        }
        IndexEntry entry = IndexEntry.of(stream, version, position);
        memIndex.add(entry);
        if(memIndex.size() >= flushThreshold) {
            writeToDisk();
        }
    }

    private void writeToDisk() {
        logger.info("Writing index to disk");
        if(memIndex.isEmpty()) {
            return;
        }
        for (IndexEntry indexEntry : memIndex) {
            diskIndex.append(indexEntry);
        }
        diskIndex.roll();
        memIndex = new MemIndex();
    }

    @Override
    public int version(long stream) {
        int version = memIndex.version(stream);
        if (version > 0) {
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
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator(range), Spliterator.ORDERED), false);
    }

    @Override
    public Stream<IndexEntry> stream() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator(), Spliterator.ORDERED), false);
    }

    @Override
    public Iterator<IndexEntry> iterator(Range range) {
        Iterator<IndexEntry> cacheIterator = memIndex.iterator(range);
        Iterator<IndexEntry> diskIterator = diskIndex.iterator(range);

        return Iterators.concat(Arrays.asList(cacheIterator, diskIterator));
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
    public Iterator<IndexEntry> iterator() {
        return Iterators.concat(Arrays.asList(diskIndex.iterator(), memIndex.iterator()));
    }

    @Override
    public void flush()  {
        writeToDisk();
    }
}