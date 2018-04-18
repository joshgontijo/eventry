package io.joshworks.fstore.es.index;

import io.joshworks.fstore.core.io.DiskStorage;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.SortedSet;

public class TableIndex implements Searchable {

    private MemIndex memIndex = new MemIndex();
    private final List<SegmentIndex> segmentIndexes = new LinkedList<>();

    public void add(long stream, int version, long position) {
        memIndex.add(stream, version, position);
    }

    @Override
    public SortedSet<IndexEntry> range(Range range) {
        SortedSet<IndexEntry> entries = memIndex.range(range);
        for (SegmentIndex segmentIndex : segmentIndexes) {
            entries.addAll(segmentIndex.range(range));
        }
        return entries;
    }

    @Override
    public Optional<IndexEntry> lastOfStream(long stream) {
        Optional<IndexEntry> indexEntry = memIndex.lastOfStream(stream);
        for (SegmentIndex segmentIndex : segmentIndexes) {
            Optional<IndexEntry> found = segmentIndex.lastOfStream(stream);
            if(found.isPresent()) {
                indexEntry = found;
            }
        }
        return indexEntry;
    }


    public int size() {
        return memIndex.size();
    }

    public void flush(File directory, String name)  {
        String segmentName = name.split("\\.")[0] + ".idx";

        SegmentIndex segmentIndex = SegmentIndex.write(memIndex, new DiskStorage(new File(directory, segmentName)));
        segmentIndexes.add(segmentIndex);
        memIndex = new MemIndex();
    }
}
