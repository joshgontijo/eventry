package io.joshworks.fstore.es.index;

import io.joshworks.fstore.core.io.MMapStorage;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.es.utils.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class TableIndex implements Index {

    private static final Logger logger = LoggerFactory.getLogger(TableIndex.class);

    private MemIndex memIndex = new MemIndex();
    private final LinkedList<SegmentIndex> segmentIndexes = new LinkedList<>();

    public void add(long stream, int version, long position) {
        if (version <= 0) {
            throw new IllegalArgumentException("Version must be greater than zero");
        }
        if (position < 0) {
            throw new IllegalArgumentException("Position must be greater than zero");
        }

        memIndex.add(stream, version, position);
    }

    @Override
    public int version(long stream) {
        int version = memIndex.version(stream);
        if (version > 0) {
            return version;
        }

        Iterator<SegmentIndex> reverse = segmentIndexes.descendingIterator();
        while (reverse.hasNext()) {
            SegmentIndex previous = reverse.next();
            int v = previous.version(stream);
            if (v > 0) {
                return v;
            }
        }
        return 0;
    }

    @Override
    public long size() {
        long segmentsSize = segmentIndexes.stream().mapToLong(SegmentIndex::size).sum();
        return segmentsSize + memIndex.size();
    }

    public long inMemoryItems() {
        return memIndex.size();
    }

    public void flush(File directory, String segmentName) {
        String indexName = segmentName.split("\\.")[0] + ".idx";

        logger.info("Flushing index to {}", indexName);
        Storage storage = new MMapStorage(new File(directory, indexName), FileChannel.MapMode.READ_WRITE);
//        Storage storage = new DiskStorage(new File(directory, indexName));
        SegmentIndex segmentIndex = SegmentIndex.write(memIndex, storage);


        segmentIndexes.add(segmentIndex);
        memIndex = new MemIndex();
    }

    @Override
    public void close() {
        memIndex.close();
        for (SegmentIndex segmentIndex : segmentIndexes) {
            logger.info("Closing {}", segmentIndex);
            segmentIndex.close();
        }
    }

    @Override
    public Stream<IndexEntry> stream() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator(), Spliterator.ORDERED), false);
    }

    @Override
    public Stream<IndexEntry> stream(Range range) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator(range), Spliterator.ORDERED), false);
    }

    @Override
    public Iterator<IndexEntry> iterator(Range range) {
        List<Iterator<IndexEntry>> iterators = new ArrayList<>();

        Iterator<IndexEntry> cacheIterator = memIndex.iterator(range);

        iterators.add(cacheIterator);
        for (SegmentIndex next : segmentIndexes) {
            iterators.add(next.iterator(range));
        }

        return Iterators.concat(iterators);
    }

    @Override
    public Optional<IndexEntry> get(long stream, int version) {
        Optional<IndexEntry> fromMemory = memIndex.get(stream, version);
        if (fromMemory.isPresent()) {
            return fromMemory;
        }
        for (SegmentIndex next : segmentIndexes) {
            Optional<IndexEntry> fromDisk = next.get(stream, version);
            if (fromDisk.isPresent()) {
                return fromDisk;
            }
        }
        return Optional.empty();
    }

    @Override
    public Iterator<IndexEntry> iterator() {
        List<Iterator<IndexEntry>> iterators = new ArrayList<>();

        for (SegmentIndex next : segmentIndexes) {
            iterators.add(next.iterator());
        }
        iterators.add(memIndex.iterator());

        return Iterators.concat(iterators);
    }
}