package io.joshworks.fstore.es.index;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.es.index.filter.BloomFilter;
import io.joshworks.fstore.es.utils.Memory;
import io.joshworks.fstore.index.filter.Hash;
import io.joshworks.fstore.serializer.Serializers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * ---- HEADER ----
 * TODO MD5 checksum -> 16bytes
 * entryCount -> 4bytes
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
public class SegmentIndex implements Searchable, Closeable {

    private static final Logger logger = LoggerFactory.getLogger(SegmentIndex.class);

    static final int HEADER_SIZE = 16;
    private static final Serializer<IndexEntry> indexEntrySerializer = new IndexKeySerializer();
    private static final Serializer<Midpoint> midpointSerializer = new MidpointSerializer();

    private final BloomFilter<Long> filter;

    private final int entryCount;

    private final Storage storage;
    final Midpoint[] midpoints;

    private SegmentIndex(Storage storage, Midpoint[] midpoints, int entryCount, BloomFilter<Long> filter) {
        this.storage = storage;
        this.midpoints = midpoints;
        this.entryCount = entryCount;
        this.filter = filter;
    }

    public Stream<IndexEntry> stream(Range range) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public List<IndexEntry> range(Range range) {
        if (outOfRange(range)) {
            return new ArrayList<>();
        }
        if(!filter.contains(range.stream)) {
            return new ArrayList<>();
        }

        IndexEntry start = range.start();
        IndexEntry end = range.end();

        int midpointIdx = getMidpointIdx(start);
        Midpoint midpoint = midpoints[midpointIdx];

        List<IndexEntry> indexEntries = new ArrayList<>();
        long position = midpoint.position;

        do {
            List<IndexEntry> loaded = readPage(position);
            if (loaded.isEmpty()) {
                return indexEntries;
            }

            int startIdx = Collections.binarySearch(loaded, start);
            startIdx = startIdx < 0 ? Math.abs(startIdx) - 1 : startIdx;

            for (int i = startIdx; i < loaded.size(); i++) {
                IndexEntry indexEntry = loaded.get(i);
                if (indexEntry.compareTo(start) < 0) {
                    continue; //skip
                }
                if (indexEntry.compareTo(end) >= 0) {
                    return indexEntries;
                }
                indexEntries.add(indexEntry);
            }

            position += IndexEntry.BYTES * loaded.size();
        } while (position <= lastMidpoint().position);

        return indexEntries;
    }

    @Override
    public Optional<IndexEntry> latestOfStream(long stream) {
        Range range = Range.allOf(stream);
        if (outOfRange(range)) {
            return Optional.empty();
        }
        if(!filter.contains(range.stream)) {
            return Optional.empty();
        }

        IndexEntry start = range.start();
        IndexEntry end = range.end();

        int midpointIdx = getMidpointIdx(end);

        Midpoint lowBound = midpoints[midpointIdx];

        IndexEntry latest = null;
        long position = lowBound.position;
        do {

            List<IndexEntry> loaded = readPage(position);
            if (loaded.isEmpty()) {
                return Optional.empty();
            }

            int startIdx = Collections.binarySearch(loaded, start);
            startIdx = startIdx < 0 ? Math.abs(startIdx) - 1 : startIdx;

            for (int i = startIdx; i < loaded.size(); i++) {
                IndexEntry indexEntry = loaded.get(i);
                if (indexEntry.compareTo(start) < 0) {
                    continue; //skip
                }
                if (indexEntry.compareTo(end) >= 0) {
                    return Optional.ofNullable(latest);
                }
                latest = indexEntry;
            }

            position += IndexEntry.BYTES * loaded.size();
        } while (position <= lastMidpoint().position);

        return Optional.ofNullable(latest);
    }

    private int getMidpointIdx(IndexEntry entry) {
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

    private IndexEntry loadEntry(long startPosition) {
        ByteBuffer bb = ByteBuffer.allocate(IndexEntry.BYTES);
        storage.read(startPosition, bb);
        bb.flip();
        return indexEntrySerializer.fromBytes(bb);
    }

    List<IndexEntry> readPage(long startPosition) {
        //out of range
        if (startPosition < HEADER_SIZE) {
            throw new IllegalArgumentException("Position must be greater or equals to " + HEADER_SIZE);
        }
        //out of range
        if (startPosition >= (lastMidpoint().position + IndexEntry.BYTES)) {
            throw new IllegalArgumentException("Max position " + startPosition + " must be less than " + ((entryCount * IndexEntry.BYTES) + HEADER_SIZE));
        }

        //not aligned position
        if ((startPosition - HEADER_SIZE) % IndexEntry.BYTES != 0) {
            throw new IllegalArgumentException("Position " + startPosition + " is not aligned with IndexEntry position of " + IndexEntry.BYTES);
        }

        int entryIdx = (int) (startPosition / IndexEntry.BYTES);

        int numKeys = Math.min(keysPerPage(), entryCount - entryIdx);
        ByteBuffer bb = ByteBuffer.allocate(numKeys *  IndexEntry.BYTES);
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
        return (int) Math.floor(Memory.PAGE_SIZE / IndexEntry.BYTES);
    }

    public static SegmentIndex write(MemIndex memIndex, Storage storage) {
        logger.info("Writing {} index entries disk", memIndex.size);

        long start = System.currentTimeMillis();

        int cachingFactor = 1;

        int maxKeysPerPage = keysPerPage() * cachingFactor;
        int totalMidPoints = Math.max(2, memIndex.index.size() * IndexEntry.BYTES / maxKeysPerPage);

        int bodySize = memIndex.index.size() * IndexEntry.BYTES;
        int footerSize = totalMidPoints * Midpoint.BYTES;

        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE + bodySize + footerSize);

        List<Midpoint> midpoints = new ArrayList<>();
        buffer.position(HEADER_SIZE);

        //first midpoint
        midpoints.add(new Midpoint(memIndex.index.first(), buffer.position()));


        BloomFilter<Long> filter = new BloomFilter<>(memIndex.index.size(), 0.3, Hash.Murmur64(Serializers.LONG));

        int mpCache = 0;
        for (IndexEntry index : memIndex.index) {
            indexEntrySerializer.writeTo(index, buffer);

            //TODO no need to serialize again prior to hashing
            //TODO same stream will be hashed multiple times, no a problem though
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
            midpoints.add(new Midpoint(memIndex.index.last(), buffer.position() - IndexEntry.BYTES));
        }

        long footerOffset = buffer.position();
        //footer
        for (Midpoint midpoint : midpoints) {
            midpointSerializer.writeTo(midpoint, buffer);
        }

        //header
        buffer.position(0);
        buffer.putInt(memIndex.index.size());
        buffer.putLong(footerOffset);
        buffer.putInt(midpoints.size());

        buffer.position(0);

        storage.write(buffer);

        logger.info("Index written to disk, took {}ms", System.currentTimeMillis() - start);
        return new SegmentIndex(storage, midpoints.toArray(new Midpoint[midpoints.size()]), memIndex.index.size(), filter);
    }

    public static SegmentIndex load(Storage storage) {

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
        return null; //TODO
    }

    public int entries() {
        return entryCount;
    }

    public int midpointCount() {
        return midpoints.length;
    }

    @Override
    public void close() {
        try {
            storage.close();
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }
}
