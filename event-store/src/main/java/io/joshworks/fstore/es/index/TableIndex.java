package io.joshworks.fstore.es.index;

import io.joshworks.fstore.core.io.MMapStorage;
import io.joshworks.fstore.core.io.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

public class TableIndex implements Index {

    private static final Logger logger = LoggerFactory.getLogger(TableIndex.class);

    private MemIndex memIndex = new MemIndex();
    private final List<SegmentIndex> segmentIndexes = new LinkedList<>();

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
    public List<IndexEntry> range(Range range) {
        List<IndexEntry> entries = memIndex.range(range);
        for (SegmentIndex segmentIndex : segmentIndexes) {
            List<IndexEntry> fromDisk = segmentIndex.range(range);
            entries.addAll(fromDisk);
        }
        return entries;
    }

    @Override
    public Optional<IndexEntry> latestOfStream(long stream) {
        Optional<IndexEntry> indexEntry = memIndex.latestOfStream(stream);
        for (SegmentIndex segmentIndex : segmentIndexes) {
            Optional<IndexEntry> found = segmentIndex.latestOfStream(stream);
            if (found.isPresent()) {
                indexEntry = found;
            }
        }
        return indexEntry;
    }


    public int size() {
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
    public Iterator<IndexEntry> iterator(Range range) {
        return null;
    }

    @Override
    public Optional<IndexEntry> get(long stream, int version) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Iterator<IndexEntry> iterator() {
        return null;
    }
}
