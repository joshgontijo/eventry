package io.joshworks.fstore.es.index;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.es.utils.Memory;

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
public class SegmentIndex implements Searchable {

    private static final int HEADER_SIZE = 16;
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
        if(outOfRange(range)) {
            return new TreeSet<>();
        }

        int idx = Arrays.binarySearch(midpoints, range.start());
        if(idx < 0) {
            idx *= (idx * -1) - 1;
        }

        SortedSet<IndexEntry> indexEntries = new TreeSet<>();
        long position = midpoints[idx].position;
        while(true) {

            SortedSet<IndexEntry> loaded = loadPage(position);
            if(loaded.isEmpty()) {
                return indexEntries;
            }

            for (IndexEntry indexEntry : loaded) {
                if(indexEntry.compareTo(range.start()) < 0) {
                    continue; //skip
                }
                if(indexEntry.compareTo(range.end()) >= 0) {
                    return indexEntries;
                }
                indexEntries.add(indexEntry);
            }

            position += IndexEntry.BYTES * loaded.size();
        }

    }

    @Override
    public Optional<IndexEntry> lastOfStream(long stream) {
        Range range = Range.allOf(stream);
        if(outOfRange(range)) {
            return Optional.empty();
        }

        int idx = Arrays.binarySearch(midpoints, range.start());
        if(idx < 0) {
            idx *= (idx * -1) - 1;
        }

        IndexEntry latest = null;
        long position = midpoints[idx].position;
        while(true) {

            SortedSet<IndexEntry> loaded = loadPage(position);
            if(loaded.isEmpty()) {
                return Optional.empty();
            }

            for (IndexEntry indexEntry : loaded) {
                if(indexEntry.compareTo(range.start()) < 0) {
                    continue; //skip
                }
                if(indexEntry.compareTo(range.end()) >= 0) {
                    return Optional.ofNullable(latest);
                }
                latest = indexEntry;
            }

            position += IndexEntry.BYTES * loaded.size();
        }
    }

    private boolean outOfRange(Range range){
        return range.start().compareTo(last()) > 0 || range.end().compareTo(first()) < 0;
    }

    public IndexEntry first() {
        return midpoints[0].key;
    }

    public IndexEntry last() {
        return midpoints[midpoints.length - 1].key;
    }

    private IndexEntry loadEntry(long startPosition) {
        ByteBuffer bb = ByteBuffer.allocate(IndexEntry.BYTES);
        storage.read(startPosition, bb);
        bb.flip();
        return indexEntrySerializer.fromBytes(bb);
    }

    private SortedSet<IndexEntry> loadPage(long startPosition) {
        //TODO how to load the keys ? page by page or larger chunks ?
        //SOLUTION: FIND THE HIGHER BOUND:
        //WHILE HIGHER BOUND IS GREATER THAN PAGE SIZE, LOAD PAGE SIZE, OTHERWISE READ THE SPECIFIC SIZE OF ENTRIES
        int maxKeysPerPage = (int) Math.floor(Memory.PAGE_SIZE / IndexEntry.BYTES);
        ByteBuffer bb = ByteBuffer.allocate(Math.min(maxKeysPerPage, entryCount) * IndexEntry.BYTES);
        storage.read(startPosition, bb);
        bb.flip();

        SortedSet<IndexEntry> entries = new TreeSet<>();
        while(bb.hasRemaining()) {
            entries.add(indexEntrySerializer.fromBytes(bb));
        }

        return entries;

    }

//    public static void main(String[] args) {
//        int cachingFactor = 1; //one per page
//        int pageSize = 4096;
//        int maxKeysPerPage = (int) Math.floor(pageSize / IndexEntry.BYTES);
//        int totalMidPoints = 1000000 * IndexEntry.BYTES / pageSize;
//
//
//        System.out.println(pageSize);
//        System.out.println(maxKeysPerPage);
//        System.out.println(totalMidPoints);
//
//    }

    public static SegmentIndex write(MemIndex memIndex, Storage storage) {
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
}
