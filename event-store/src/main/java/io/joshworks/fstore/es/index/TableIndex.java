package io.joshworks.fstore.es.index;

import io.joshworks.fstore.core.io.MMapStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;

public class TableIndex implements Searchable, Closeable {

    private static final Logger logger = LoggerFactory.getLogger(TableIndex.class);

    private MemIndex memIndex = new MemIndex();
    private final List<SegmentIndex> segmentIndexes = new LinkedList<>();

    public void add(long stream, int version, long position) {
        memIndex.add(stream, version, position);
    }

    @Override
    public SortedSet<IndexEntry> range(Range range) {
        SortedSet<IndexEntry> fromMemIndex = memIndex.range(range);
        SortedSet<IndexEntry> entries = new TreeSet<>(fromMemIndex);
        for (SegmentIndex segmentIndex : segmentIndexes) {
            SortedSet<IndexEntry> fromDisk = segmentIndex.range(range);
            entries.addAll(fromDisk);
        }
        return entries;
    }

    @Override
    public Optional<IndexEntry> latestOfStream(long stream) {
        Optional<IndexEntry> indexEntry = memIndex.latestOfStream(stream);
        for (SegmentIndex segmentIndex : segmentIndexes) {
            Optional<IndexEntry> found = segmentIndex.latestOfStream(stream);
            if(found.isPresent()) {
                indexEntry = found;
            }
        }
        return indexEntry;
    }


    public int size() {
        return memIndex.size();
    }

    public void flush(File directory, String segmentName)  {
        String indexName = segmentName.split("\\.")[0] + ".idx";

        logger.info("Flushing index to {}", indexName);
        SegmentIndex segmentIndex = SegmentIndex.write(memIndex, new MMapStorage(new File(directory, indexName), FileChannel.MapMode.READ_WRITE));
        segmentIndexes.add(segmentIndex);
        memIndex = new MemIndex();
    }

    @Override
    public void close()  {
        memIndex.close();
        for (SegmentIndex segmentIndex : segmentIndexes) {
            logger.info("Closing {}", segmentIndex);
            segmentIndex.close();
        }

    }
}
