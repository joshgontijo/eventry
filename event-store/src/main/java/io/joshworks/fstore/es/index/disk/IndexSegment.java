package io.joshworks.fstore.es.index.disk;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.DataReader;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.es.index.Index;
import io.joshworks.fstore.es.index.IndexEntry;
import io.joshworks.fstore.es.index.Range;
import io.joshworks.fstore.es.index.filter.BloomFilter;
import io.joshworks.fstore.es.index.midpoint.Midpoint;
import io.joshworks.fstore.es.index.midpoint.Midpoints;
import io.joshworks.fstore.es.utils.Iterators;
import io.joshworks.fstore.index.filter.Hash;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.block.BlockSegment;
import io.joshworks.fstore.serializer.Serializers;

import java.io.File;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class IndexSegment extends BlockSegment<IndexEntry, FixedSizeEntryBlock<IndexEntry>> implements Index {

    private IndexEntry lastInserted;

    BloomFilter<Long> filter = new BloomFilter<>(10000, new Hash.Murmur64<>(Serializers.LONG));
    final Midpoints midpoints;

    public IndexSegment(Storage storage,
                        Serializer<FixedSizeEntryBlock<IndexEntry>> serializer,
                        DataReader reader,
                        long position,
                        boolean readOnly,
                        File directory) {
        super(storage, serializer, reader, position, readOnly);
        this.midpoints = new Midpoints(directory, name());
    }

    @Override
    protected long writeBlock() {
        FixedSizeEntryBlock<IndexEntry> block = currentBlock();
        if (block.isEmpty()) {
            return position();
        }
        midpoints.add(new Midpoint(block.first(), position()));
        return super.writeBlock();
    }

    @Override
    public long append(IndexEntry data) {
        lastInserted = data;
        filter.add(data.stream);
        return super.append(data);
    }

    @Override
    public void roll() {
        FixedSizeEntryBlock<IndexEntry> block = currentBlock();
        long position = position();
        if (!block.isEmpty()) {
            midpoints.add(new Midpoint(block.first(), position));
            writeBlock();
        }
        if (lastInserted != null) {
            midpoints.add(new Midpoint(lastInserted, position));
            midpoints.write();
        }
    }

    @Override
    public void flush() {
        //TODO add bloom filter
        super.flush(); //flush super first, so writeBlock is called
        midpoints.write();

    }

    @Override
    public void close() {
        super.close();
    }

    @Override
    public Iterator<IndexEntry> iterator(Range range) {
        if (!hasEntries(range)) {
            return Iterators.empty();
        }

        Midpoint lowBound = midpoints.getMidpointFor(range.start());
        if (lowBound == null) {
            return Iterators.empty();
        }

        LogIterator<IndexEntry> logIterator = iterator(lowBound.position);
        return new RangeIndexEntryIterator(range, logIterator);
    }

    private boolean hasEntries(Range range) {
        return midpoints.inRange(range) && filter.contains(range.stream);
    }


    @Override
    public Stream<IndexEntry> stream(Range range) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator(range), Spliterator.ORDERED), false);
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

        IndexEntry end = range.end();
        Midpoint lowBound = midpoints.getMidpointFor(end);
        if (lowBound == null) {//false positive on the bloom filter and entry was within range of this segment
            return 0;
        }

        FixedSizeEntryBlock<IndexEntry> block = getBlock(lowBound.position);
        List<IndexEntry> entries = block.entries();
        int idx = Collections.binarySearch(entries, end);
        idx = idx >= 0 ? idx : Math.abs(idx) - 2;
        IndexEntry lastVersion = entries.get(idx);
        if (lastVersion.stream != stream) {
            throw new IllegalStateException("Found entry with wrong stream");
        }
        return lastVersion.version;
    }

    @Override
    protected FixedSizeEntryBlock<IndexEntry> createBlock() {
        return new FixedSizeEntryBlock<>(new IndexEntrySerializer(), 204, IndexEntry.BYTES);
    }

    private static final class RangeIndexEntryIterator implements Iterator<IndexEntry> {

        private final IndexEntry end;
        private final IndexEntry start;
        private final Iterator<IndexEntry> segmentIterator;
        private IndexEntry current;

        private RangeIndexEntryIterator(Range range, Iterator<IndexEntry> segmentIterator) {
            this.end = range.end();
            this.start = range.start();
            this.segmentIterator = segmentIterator;

            //initial load skipping less than start
            while (segmentIterator.hasNext()) {
                IndexEntry next = segmentIterator.next();
                if (next.greatOrEqualsTo(start)) {
                    current = next;
                    break;
                }
            }
        }

        @Override
        public boolean hasNext() {
            return current != null && current.lessThan(end);
        }

        @Override
        public IndexEntry next() {
            if (current == null) {
                throw new NoSuchElementException();
            }
            IndexEntry curr = current;
            current = segmentIterator.hasNext() ? segmentIterator.next() : null;
            return curr;
        }
    }

}
