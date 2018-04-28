package io.joshworks.fstore.es.index;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.MMapStorage;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.es.index.filter.BloomFilter;
import io.joshworks.fstore.es.utils.Iterators;
import io.joshworks.fstore.es.utils.Memory;
import io.joshworks.fstore.index.filter.Hash;
import io.joshworks.fstore.serializer.Serializers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * ---- HEADER ----
 * TODO MD5 checksum -> 16bytes
 * length -> 4bytes
 * TODO ? entrySize -> 8bytes
 * footer offset -> 8bytes
 * midpointCount -> 4bytes
 * <p>
 * ---- ENTRIES ----
 * [entry][entry]...
 * <p>
 * ---- FOOTER -----
 * [midpoint][midpoint]...
 * <p>
 * <p>
 * <p>
 * EachSegment index file MUST have a Midpoint of the start and end
 * Example:
 * Given a dummy array of numbers
 * [0,1,2,3,4,5,6,7,8,9]
 * <p>
 * midpointsDistance = 2
 * MIDPOINTS = [0,3,6,9]
 * <p>
 * The client code is responsible for ensuring the number will match
 */
public class SegmentIndex implements Index {

    private static final Logger logger = LoggerFactory.getLogger(SegmentIndex.class);

    static final int HEADER_SIZE = 16;
    private static final Serializer<IndexEntry> indexEntrySerializer = new IndexKeySerializer();
    private static final Serializer<Midpoint> midpointSerializer = new MidpointSerializer();

    private final BloomFilter<Long> filter;

    private final int size;

    private final Storage storage;
    final Midpoint[] midpoints;

    private SegmentIndex(Storage storage, Midpoint[] midpoints, int size, BloomFilter<Long> filter) {
        this.storage = storage;
        this.midpoints = midpoints;
        this.size = size;
        this.filter = filter;
    }

    @Override
    public int version(long stream) {
        Range range = Range.allOf(stream);
        if(!hasEntries(range)) {
            return 0;
        }

        IndexEntry start = range.start();
        IndexEntry end = range.end();

        int midpointIdx = getMidpointIdx(midpoints, end);
        Midpoint lowBound = midpoints[midpointIdx];

        int version = 0;
        long position = lowBound.position;
        do {

            List<IndexEntry> loaded = readPage(position);
            if (loaded.isEmpty()) {
                return version;
            }

            int startIdx = Collections.binarySearch(loaded, start);
            startIdx = startIdx < 0 ? Math.abs(startIdx) - 1 : startIdx;

            for (int i = startIdx; i < loaded.size(); i++) {
                IndexEntry indexEntry = loaded.get(i);
                if (indexEntry.lessThan(start)) {
                    continue; //skip
                }
                if (indexEntry.greatOrEqualsTo(end)) {
                    return version;
                }
                version = indexEntry.version;
            }

            position += IndexEntry.BYTES * loaded.size();
        } while (position <= lastMidpoint().position);

        return version;
    }

    static int getMidpointIdx(Midpoint[] midpoints, IndexEntry entry) {
        int idx = Arrays.binarySearch(midpoints, entry);
        if (idx < 0) {
            idx = Math.abs(idx) - 2; // -1 for the actual position, -1 for the offset where to start scanning
            idx = idx < 0 ? 0 : idx;
        }
        if (idx >= midpoints.length || idx < 0) {
            throw new IllegalStateException("Got index " + idx + " midpoints position: " + midpoints.length);
        }
        return idx;
    }

    private boolean outOfRange(Range range) {
        return range.start().compareTo(last()) > 0 || range.end().compareTo(first()) < 0;
    }

    public IndexEntry first() {
        return firstMidpoint().key;
    }

    public IndexEntry last() {
        return lastMidpoint().key;
    }

    private Midpoint firstMidpoint() {
        return midpoints[0];
    }

    private Midpoint lastMidpoint() {
        return midpoints[midpoints.length - 1];
    }

    List<IndexEntry> readPage(long startPosition) {
        //out of range
        if (startPosition < HEADER_SIZE) {
            throw new IllegalArgumentException("Position must be greater or equals to " + HEADER_SIZE);
        }
        //out of range
        if (startPosition >= (lastMidpoint().position + IndexEntry.BYTES)) {
            throw new IllegalArgumentException("Max position " + startPosition + " must be less than " + ((size * IndexEntry.BYTES) + HEADER_SIZE));
        }

        //not aligned position
        if ((startPosition - HEADER_SIZE) % IndexEntry.BYTES != 0) {
            throw new IllegalArgumentException("Position " + startPosition + " is not aligned with IndexEntry position of " + IndexEntry.BYTES);
        }

        int entryIdx = (int) ((startPosition - HEADER_SIZE) / IndexEntry.BYTES);

        int numKeys = Math.min(keysPerPage(), (size - entryIdx));
        ByteBuffer bb = ByteBuffer.allocate(numKeys * IndexEntry.BYTES);
        storage.read(startPosition, bb);
        bb.flip();

        List<IndexEntry> entries = new ArrayList<>(numKeys);
        while (bb.hasRemaining()) {
            IndexEntry indexEntry = indexEntrySerializer.fromBytes(bb);
            entries.add(indexEntry);
        }

        return entries;
    }

    private static int keysPerPage() {
        return Memory.PAGE_SIZE / IndexEntry.BYTES;
    }

    public static SegmentIndex write(MemIndex memIndex, File dest) {
        logger.info("Writing {} index entries to disk", memIndex.size());

        long start = System.currentTimeMillis();

        int cachingFactor = 1;

        int maxKeysPerPage = keysPerPage() * cachingFactor;
        int totalMidPoints = Math.max(2, memIndex.size() * IndexEntry.BYTES / maxKeysPerPage);

        int bodySize = memIndex.size() * IndexEntry.BYTES;
        int footerSize = totalMidPoints * Midpoint.BYTES;


        int bufferSize = HEADER_SIZE + bodySize + footerSize;
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);

        logger.info("Index file size {}", bufferSize);

        Storage storage = new MMapStorage(dest, bufferSize, FileChannel.MapMode.READ_WRITE);


        List<Midpoint> midpoints = new ArrayList<>();
        buffer.position(HEADER_SIZE);


        BloomFilter<Long> filter = new BloomFilter<>(memIndex.size(), 0.3, Hash.Murmur64(Serializers.LONG));

        //first midpoint
        midpoints.add(new Midpoint(memIndex.iterator().next(), buffer.position()));

        Iterator<IndexEntry> iterator = memIndex.iterator();
        IndexEntry last = null;
        int mpCache = 0;
        while(iterator.hasNext()) {
            IndexEntry index = iterator.next();
            last = index;
            indexEntrySerializer.writeTo(index, buffer);

            //TODO no need to serialize again prior to hashing
            //TODO same stream will be hashed multiple times, not a problem though
            filter.add(index.stream);

            if (mpCache == maxKeysPerPage) {
                int address = buffer.position() - IndexEntry.BYTES;
                midpoints.add(new Midpoint(index, address));
                mpCache = 0;
            }
            mpCache++;
        }

        //last midpoint
        if (mpCache != 0) {
            midpoints.add(new Midpoint(last, buffer.position() - IndexEntry.BYTES));
        }

        long footerOffset = buffer.position();
        //footer
        for (Midpoint midpoint : midpoints) {
            midpointSerializer.writeTo(midpoint, buffer);
        }

        //header
        buffer.position(0);
        buffer.putInt(memIndex.size());
        buffer.putLong(footerOffset);
        buffer.putInt(midpoints.size());

        buffer.position(0);

        storage.write(buffer);

        logger.info("Index written to disk, took {}ms", System.currentTimeMillis() - start);
        return new SegmentIndex(storage, midpoints.toArray(new Midpoint[midpoints.size()]), memIndex.size(), filter);
    }

    public static SegmentIndex load(File indexFile) {

        Storage storage = new MMapStorage(indexFile, indexFile.length(), FileChannel.MapMode.READ_WRITE);

        ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
        storage.read(0, header);

        header.flip();

        int entriesSize = header.getInt();
        long footerOffset = header.getLong();
        int midpointSize = header.getInt();

        ByteBuffer midpointsBuffer = ByteBuffer.allocate(midpointSize * Midpoint.BYTES);
        storage.read(footerOffset, midpointsBuffer);
        midpointsBuffer.flip();

        Midpoint[] midpoints = new Midpoint[midpointSize];
        for (int i = 0; i < midpointSize; i++) {
            midpoints[i] = midpointSerializer.fromBytes(midpointsBuffer);
        }

        BloomFilter<Long> filter = loadBloomFilter();

        return new SegmentIndex(storage, midpoints, entriesSize, filter);
    }

    private static BloomFilter<Long> loadBloomFilter() {
        //TODO IMPLEMENT BLOOM FILTER LOADING
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public void close() {
        try {
            storage.close();
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    private boolean hasEntries(Range range) {
        return !outOfRange(range) && filter.contains(range.stream);
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
    public Iterator<IndexEntry> iterator() {
        return new FullScanIterator();
    }

    @Override
    public Iterator<IndexEntry> iterator(Range range) {
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
        return new MultiPageRangeIterator(range, start, firstEntryPos);
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
            throw new IllegalStateException("More than one event found for fromStream " + stream + ", version " + version);
        }
        return Optional.of(next);

    }

    private class MultiPageRangeIterator implements Iterator<IndexEntry> {

        private final IndexEntry end;

        private long position;
        private final Queue<IndexEntry> entries;

        private MultiPageRangeIterator(Range range, List<IndexEntry> initialData, long initialPosition) {
            this.end = range.end();
            if (initialData.isEmpty()) {
                //just to avoid having to read another page
                throw new IllegalStateException("Initial data cannot be empty");
            }
            entries = new ConcurrentLinkedQueue<>(initialData);
            this.position = initialPosition;
        }

        @Override
        public boolean hasNext() {
            if (!entries.isEmpty()) {
                IndexEntry next = entries.peek();
                return next.lessThan(end);
            }

            if (position > lastMidpoint().position) {
                return false;
            }

            List<IndexEntry> loaded = readPage(position);
            if (loaded.isEmpty()) {
                return false;
            }

            entries.addAll(loaded);

            IndexEntry next = entries.peek();
            return next.compareTo(end) < 0;
        }

        @Override
        public IndexEntry next() {
            this.position += IndexEntry.BYTES;
            return entries.remove();

        }
    }

    private class FullScanIterator implements Iterator<IndexEntry> {

        private long position = HEADER_SIZE;
        private final Queue<IndexEntry> entries = new ConcurrentLinkedQueue<>();

        @Override
        public boolean hasNext() {
            if (!entries.isEmpty()) {
                return true;
            }

            if (position > lastMidpoint().position) {
                return false;
            }

            List<IndexEntry> loaded = readPage(position);
            if (loaded.isEmpty()) {
                return false;
            }

            entries.addAll(loaded);
            return true;
        }

        @Override
        public IndexEntry next() {
            this.position += IndexEntry.BYTES;
            return entries.remove();
        }
    }

    @Override
    public String toString() {
        return storage.name();
    }
}