package io.joshworks.fstore.es.appender;

import io.joshworks.fstore.es.index.Index;
import io.joshworks.fstore.es.index.IndexEntry;
import io.joshworks.fstore.es.index.IndexKeySerializer;
import io.joshworks.fstore.es.index.IndexSegment;
import io.joshworks.fstore.es.index.MemIndex;
import io.joshworks.fstore.es.index.Range;
import io.joshworks.fstore.es.index.filter.BloomFilter;
import io.joshworks.fstore.es.index.midpoint.Midpoint;
import io.joshworks.fstore.es.index.midpoint.Midpoints;
import io.joshworks.fstore.es.utils.Iterators;
import io.joshworks.fstore.log.appender.Appender;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.log.block.Block;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public class EventIndex implements Index {

    private final LogAppender<FixedSizeEntryBlock<IndexEntry>> diskIndex;

    private final Map<String, Midpoints> segmentMidpoints = new HashMap<>();
    private final Map<String, BloomFilter<Long>> segmentFilters = new HashMap<>();

    private MemIndex memIndex = new MemIndex();

    public EventIndex(File rootPath) {
        File file = new File(rootPath, "index");
        this.diskIndex = Appender.builder(file, new FixedSizeBlockSerializer<>(new IndexKeySerializer(), IndexEntry.BYTES)).open();
    }

    public void write(MemIndex memIndex) {
        if (memIndex.size() == 0) {
            throw new IllegalArgumentException("MemIndex is empty");
        }

        FixedSizeEntryBlock<IndexEntry> block = newBlock();

        List<Midpoint> midpoints = new ArrayList<>();
        for (IndexEntry entry : memIndex) {
            if (block.add(entry)) {

                long blockPosition = diskIndex.append(block);
                midpoints.add(new Midpoint(block.first(), blockPosition));
                block = newBlock();
            }
        }
        if (!block.isEmpty()) {
            long blockPosition = diskIndex.append(block);
            midpoints.add(new Midpoint(block.first(), blockPosition));
        }

        midpoints.add(new Midpoint(block.last(), diskIndex.position()));

        writeMidpoints(midpoints);
    }

    private void writeMidpoints(List<Midpoint> midpoints) {
        String segmentName = diskIndex.currentSegment();
        File indexDir = diskIndex.directory().toFile();

        Midpoints segmentMidpoints = Midpoints.write(indexDir, segmentName, midpoints);
        this.segmentMidpoints.put(segmentName, segmentMidpoints);
    }


    private FixedSizeEntryBlock<IndexEntry> newBlock() {
        return new FixedSizeEntryBlock<>(new IndexKeySerializer(), 204, 20);
    }

    @Override
    public Iterator<IndexEntry> iterator(Range range) {
        List<Iterator<IndexEntry>> iterators = new ArrayList<>();

        Iterator<IndexEntry> cacheIterator = memIndex.iterator(range);

        iterators.add(cacheIterator);
        for (IndexSegment next : indexSegments) {
            iterators.add(next.iterator(range));
        }

        return Iterators.concat(iterators);


        if (!hasEntries(range)) {
            return Iterators.empty();
        }

        int midpointIdx = getMidpointIdx(midpoints, range.start());
        Midpoint lowBound = midpoints[midpointIdx];
        List<IndexEntry> loaded = readPage(lowBound.position);
        if (loaded.isEmpty()) {
            return Iterators.empty();
        }

        int startIdx = Collections.binarySearch(loaded, range.start());
        startIdx = startIdx < 0 ? Math.abs(startIdx) - 1 : startIdx;
        if (startIdx >= loaded.size()) {
            return Iterators.empty();
        }

        List<IndexEntry> start = loaded.subList(startIdx, loaded.size());
        long firstEntryPos = lowBound.position + (startIdx * IndexEntry.BYTES);
        return new IndexSegment.MultiPageRangeIterator(range, start, firstEntryPos);
    }

    @Override
    public Stream<IndexEntry> stream() {
        return diskIndex.stream().flatMap(Block::stream);
    }

    @Override
    public Stream<IndexEntry> stream(Range range) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<IndexEntry> get(long stream, int version) {
        return Optional.empty();
    }

    @Override
    public int version(long stream) {
        return 0;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public Iterator<IndexEntry> iterator() {
        return null;
    }
}
