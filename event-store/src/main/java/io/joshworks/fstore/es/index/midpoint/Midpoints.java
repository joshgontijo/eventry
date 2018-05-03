package io.joshworks.fstore.es.index.midpoint;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.MMapStorage;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.es.index.IndexEntry;
import io.joshworks.fstore.es.index.Range;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Midpoints {

    private static final Serializer<Midpoint> midpointSerializer = new MidpointSerializer();
    private final List<Midpoint> midpoints;
    private final File handler;

    private Midpoints(List<Midpoint> midpoints, File handler) {
        this.midpoints = midpoints;

        this.handler = handler;
    }

    public static Midpoints write(File indexFolder, String segmentName, List<Midpoint> midpoints) {
        int size = Midpoint.BYTES * midpoints.size();

        File midpointFile = getFile(indexFolder, segmentName);

        try(Storage storage = new MMapStorage(midpointFile, size, FileChannel.MapMode.READ_WRITE)) {
            for (Midpoint midpoint : midpoints) {
                ByteBuffer data = midpointSerializer.toBytes(midpoint);
                storage.write(data);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return new Midpoints(midpoints, midpointFile);
    }

    public static Midpoints load(File indexFolder, String segmentName) throws IOException {

        File midpointFile = getFile(indexFolder, segmentName);
        List<Midpoint> loaded = new ArrayList<>();
        try(Storage storage = new MMapStorage(midpointFile, midpointFile.length(), FileChannel.MapMode.READ_WRITE)) {
            long pos = 0;
            ByteBuffer data = ByteBuffer.allocate(Midpoint.BYTES);

            while(storage.read(pos, data) > 0) {
                Midpoint midpoint = midpointSerializer.fromBytes(data);
                loaded.add(midpoint);
                pos += Midpoint.BYTES;
                data.clear();
            }
        }

        return new Midpoints(loaded, midpointFile);
    }

    private static File getFile(File indexDir, String segmentName) {
        return new File(indexDir, segmentName + "-MIDPOINT.mdp");
    }

    int getMidpointIdx(IndexEntry entry) {
        int idx = Collections.binarySearch(midpoints, entry);
        if (idx < 0) {
            idx = Math.abs(idx) - 2; // -1 for the actual position, -1 for the offset where to start scanning
            idx = idx < 0 ? 0 : idx;
        }
        if (idx >= midpoints.size()) {
            throw new IllegalStateException("Got index " + idx + " midpoints position: " + midpoints.size());
        }
        return idx;
    }


    public void delete() {
        try {
            Files.delete(handler.toPath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean inRange(Range range) {
        return range.start().compareTo(last()) > 0 || range.end().compareTo(first()) < 0;
    }

    public IndexEntry first() {
        return firstMidpoint().key;
    }

    public IndexEntry last() {
        return lastMidpoint().key;
    }

    private Midpoint firstMidpoint() {
        return midpoints.get(0);
    }

    private Midpoint lastMidpoint() {
        return midpoints.get(midpoints.size() - 1);
    }



}
