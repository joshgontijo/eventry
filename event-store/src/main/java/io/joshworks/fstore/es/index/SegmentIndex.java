package io.joshworks.fstore.es.index;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.es.utils.Memory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;

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


    private final int entryCount;

    private final Storage storage;
    public final Midpoint[] midpoints;

    private SegmentIndex(Storage storage, Midpoint[] midpoints, int entryCount) {
        this.storage = storage;
        this.midpoints = midpoints;
        this.entryCount = entryCount;
    }

    //TODO stream results ?
    @Override
    public SortedSet<IndexEntry> range(Range range) {
        if (outOfRange(range)) {
            return new TreeSet<>();
        }

        Midpoint midpoint = midpoints[getMidpointIdx(range.start())];

        SortedSet<IndexEntry> indexEntries = new TreeSet<>();
        long position = midpoint.position;

        do {
            SortedSet<IndexEntry> loaded = loadPage(position);
            if (loaded.isEmpty()) {
                return indexEntries;
            }

            for (IndexEntry indexEntry : loaded) {
                if (indexEntry.compareTo(range.start()) < 0) {
                    continue; //skip
                }
                if (indexEntry.compareTo(range.end()) >= 0) {
                    return indexEntries;
                }
                indexEntries.add(indexEntry);
            }

            position += IndexEntry.BYTES * loaded.size();
        }while (position <= lastMidpoint().position);
        return indexEntries;

    }

    @Override
    public Optional<IndexEntry> latestOfStream(long stream) {
        Range range = Range.allOf(stream);
        if (outOfRange(range)) {
            return Optional.empty();
        }

        Midpoint midpoint = midpoints[getMidpointIdx(range.end()) - 1];

        IndexEntry latest = null;
        long position = midpoint.position;
        do {

            SortedSet<IndexEntry> loaded = loadPage(position);
            if (loaded.isEmpty()) {
                return Optional.empty();
            }

            for (IndexEntry indexEntry : loaded) {
                if (indexEntry.compareTo(range.start()) < 0) {
                    continue; //skip
                }
                if (indexEntry.compareTo(range.end()) >= 0) {
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
            throw new IllegalStateException("Got index " + idx + " midpoints size: " + midpoints.length);
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

    SortedSet<IndexEntry> loadPage(long startPosition) {
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
            throw new IllegalArgumentException("Position " + startPosition + " is not aligned with IndexEntry size of " + IndexEntry.BYTES);
        }

        //TODO how to load the keys ? page by page or larger chunks ?
        //SOLUTION: FIND THE HIGHER BOUND:
        //WHILE HIGHER BOUND IS GREATER THAN PAGE SIZE, LOAD PAGE SIZE, OTHERWISE READ THE SPECIFIC SIZE OF ENTRIES
        int maxKeysPerPage = (int) Math.floor(Memory.PAGE_SIZE / IndexEntry.BYTES);
        ByteBuffer bb = ByteBuffer.allocate(Math.min(maxKeysPerPage, entryCount) * IndexEntry.BYTES);
        storage.read(startPosition, bb);
        bb.flip();

        SortedSet<IndexEntry> entries = new TreeSet<>();
        while (bb.hasRemaining()) {
            IndexEntry indexEntry = indexEntrySerializer.fromBytes(bb);
            entries.add(indexEntry);
        }

        return entries;

    }

    public static SegmentIndex write(MemIndex memIndex, Storage storage) {
        logger.info("Writing {} index entries disk", memIndex.size);

        long start = System.currentTimeMillis();

        int cachingFactor = 1; //TODO

        int maxKeysPerPage = (int) Math.floor(Memory.PAGE_SIZE / IndexEntry.BYTES);
        int totalMidPoints = Math.max(2, memIndex.index.size() * IndexEntry.BYTES / maxKeysPerPage);


        int bodySize = memIndex.index.size() * IndexEntry.BYTES;
        int footerSize = totalMidPoints * Midpoint.BYTES;

        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE + bodySize + footerSize);


        List<Midpoint> midpoints = new ArrayList<>();
        buffer.position(HEADER_SIZE);

        //first midpoint
        midpoints.add(new Midpoint(memIndex.index.first(), buffer.position()));

        //TODO properly calculate midpoints spacing
        int mpCache = 0;
        for (IndexEntry index : memIndex.index) {
            indexEntrySerializer.writeTo(index, buffer);

            if (mpCache >= maxKeysPerPage) {
                int address = buffer.position() - IndexEntry.BYTES;
                midpoints.add(new Midpoint(index, address));
                mpCache = 0;
            }
            mpCache++;
        }

        //last midpoint
        midpoints.add(new Midpoint(memIndex.index.last(), buffer.position() - IndexEntry.BYTES));

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
        return new SegmentIndex(storage, midpoints.toArray(new Midpoint[midpoints.size()]), memIndex.index.size());
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

        return new SegmentIndex(storage, midpoints, entriesSize);
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
