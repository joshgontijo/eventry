package io.joshworks.fstore.es.appender;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.DataReader;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.es.index.Index;
import io.joshworks.fstore.es.index.IndexEntry;
import io.joshworks.fstore.es.index.IndexEntrySerializer;
import io.joshworks.fstore.es.index.MemIndex;
import io.joshworks.fstore.es.index.Range;
import io.joshworks.fstore.es.index.filter.BloomFilter;
import io.joshworks.fstore.es.index.midpoint.Midpoint;
import io.joshworks.fstore.es.index.midpoint.Midpoints;
import io.joshworks.fstore.es.utils.Iterators;
import io.joshworks.fstore.index.filter.Hash;
import io.joshworks.fstore.log.LogSegment;
import io.joshworks.fstore.log.Scanner;
import io.joshworks.fstore.serializer.Serializers;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Queue;
import java.util.stream.Stream;

public class IndexSegment extends LogSegment<FixedSizeEntryBlock<IndexEntry>> implements Index {

    private final File directory;

    private BloomFilter<Long> filter = new BloomFilter<>(10000, new Hash.Murmur64<>(Serializers.LONG));
    private Midpoints midpoints = Midpoints.EMPTY;

    public IndexSegment(Storage storage,
                        Serializer<FixedSizeEntryBlock<IndexEntry>> serializer,
                        DataReader reader,
                        long position,
                        boolean readOnly,
                        File directory) {
        super(storage, serializer, reader, position, readOnly);
        this.directory = directory;
    }

    @Override
    public long append(FixedSizeEntryBlock<IndexEntry> data) {
        throw new UnsupportedOperationException();
    }

    public void write(MemIndex memIndex) {
        if (memIndex.size() == 0) {
            throw new IllegalArgumentException("MemIndex is empty");
        }

        FixedSizeEntryBlock<IndexEntry> block = newBlock();

        List<Midpoint> segmentMidpoints = new ArrayList<>();
        IndexEntry last = null;
        for (IndexEntry entry : memIndex) {
            filter.add(entry.stream);
            last = entry;

            if (block.add(entry)) {
                long blockPosition = super.append(block);
                segmentMidpoints.add(new Midpoint(block.first(), blockPosition));
                block = newBlock();
            }
        }
        if (!block.isEmpty()) {
            long blockPosition = super.append(block);
            segmentMidpoints.add(new Midpoint(block.first(), blockPosition));

        }
        segmentMidpoints.add(new Midpoint(last, position()));


        writeMidpoints(segmentMidpoints);
    }

    private FixedSizeEntryBlock<IndexEntry> newBlock() {
        return new FixedSizeEntryBlock<>(new IndexEntrySerializer(), 204, IndexEntry.BYTES);
    }

    private void writeMidpoints(List<Midpoint> midpoints) {
        this.midpoints = Midpoints.write(this.directory, name(), midpoints);
    }

    @Override
    public Iterator<IndexEntry> iterator(Range range) {
        if (!hasEntries(range)) {
            return Iterators.empty();
        }

        Midpoint lowBound = midpoints.getMidpointFor(range.start());
        if(lowBound == null) {
            return Iterators.empty();
        }

        Scanner<FixedSizeEntryBlock<IndexEntry>> scanner = scanner(lowBound.position);
        return new RangeIndexEntryIterator(range, scanner);
    }

    private boolean hasEntries(Range range) {
        return midpoints.inRange(range) && filter.contains(range.stream);
    }


    @Override
    public Stream<IndexEntry> stream(Range range) {
        return null;
    }

    @Override
    public Optional<IndexEntry> get(long stream, int version) {
        Range range = Range.of(stream, version, version + 1);
        Iterator<IndexEntry> found = iterator(range);
        if (!found.hasNext()) {
            return Optional.empty();
        }
        IndexEntry next = found.next();
        if (found.hasNext()) {
            throw new IllegalStateException("More than one event found for stream " + stream + ", version " + version);
        }
        return Optional.of(next);
    }

    @Override
    public int version(long stream) {
        Range range = Range.allOf(stream);
        if (!hasEntries(range)) {
            return 0;
        }

        Midpoint lowBound = midpoints.getMidpointFor(range.end());
        if(lowBound == null) {//false positive on the bloom filter and entry was within range of this segment
            return 0;
        }

        Scanner<FixedSizeEntryBlock<IndexEntry>> scanner = scanner(lowBound.position);
        RangeIndexEntryIterator rangeIterator = new RangeIndexEntryIterator(range, scanner);

        int version = 0;
        while (rangeIterator.hasNext()) {

            IndexEntry next = rangeIterator.next();
            if(next.stream != stream) {
                throw new IllegalStateException("Found entry with wrong stream");
            }
            version = next.version;
        }

        return version;
    }

    @Override
    public Iterator<IndexEntry> iterator() {
        Scanner<FixedSizeEntryBlock<IndexEntry>> scanner = scanner();
        return new FullIndexEntryIterator(scanner);
    }

    private static final class RangeIndexEntryIterator implements Iterator<IndexEntry> {

        private final IndexEntry end;
        private final IndexEntry start;
        private final Iterator<FixedSizeEntryBlock<IndexEntry>> segmentIterator;
        private Queue<IndexEntry> entries = new LinkedList<>();

        private RangeIndexEntryIterator(Range range, Iterator<FixedSizeEntryBlock<IndexEntry>> segmentIterator) {
            this.end = range.end();
            this.start = range.start();
            this.segmentIterator = segmentIterator;

            //initial load skipping less than
            if (segmentIterator.hasNext()) {
                FixedSizeEntryBlock<IndexEntry> firstBlock = segmentIterator.next();
                entries = firstBlock.queueEntries();
                if (!entries.isEmpty()) {
                    while (entries.element().lessThan(start)) {
                        entries.remove();
                    }
                }
            }
        }

        @Override
        public boolean hasNext() {
            if (!entries.isEmpty()) {
               return entries.element().lessThan(end);
            }

            if (!segmentIterator.hasNext()) {
                return false;
            }
            FixedSizeEntryBlock<IndexEntry> block = segmentIterator.next();
            entries = block.queueEntries();

            if (entries.isEmpty()) {
                return false;
            }

            return entries.element().lessThan(end);

        }

        @Override
        public IndexEntry next() {
            if (entries.isEmpty()) {
                throw new NoSuchElementException();
            }
            return entries.poll();
        }

    }

    private static final class FullIndexEntryIterator implements Iterator<IndexEntry> {

        private final Iterator<FixedSizeEntryBlock<IndexEntry>> segmentIterator;
        private Queue<IndexEntry> entries = new LinkedList<>();

        private FullIndexEntryIterator(Iterator<FixedSizeEntryBlock<IndexEntry>> segmentIterator) {
            this.segmentIterator = segmentIterator;
        }

        @Override
        public boolean hasNext() {
            if (!entries.isEmpty()) {
                return true;
            }
            if (!segmentIterator.hasNext()) {
                return false;
            }
            FixedSizeEntryBlock<IndexEntry> block = segmentIterator.next();
            entries = block.queueEntries();

            return !entries.isEmpty();
        }

        @Override
        public IndexEntry next() {
            if (entries.isEmpty()) {
                throw new NoSuchElementException();
            }
            return entries.poll();
        }
    }

}
