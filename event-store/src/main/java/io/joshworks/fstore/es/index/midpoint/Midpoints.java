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
import java.util.Objects;

public class Midpoints {

    private static final Serializer<Midpoint> midpointSerializer = new MidpointSerializer();
    private final List<Midpoint> midpoints = new ArrayList<>();
    private final File handler;

    public Midpoints(File indexDir, String segmentFileName) {
        this.handler = getFile(indexDir, segmentFileName);
        if (handler.exists()) {
            load();
        }
    }

    public void add(Midpoint midpoint) {
        Objects.requireNonNull(midpoint, "Midpoint cannot be null");
        Objects.requireNonNull(midpoint.key, "Midpoint entry cannot be null");
        this.midpoints.add(midpoint);
    }

    public void write() {
        if(midpoints.isEmpty()) {
            return;
        }
        int size = Midpoint.BYTES * midpoints.size();

        try (Storage storage = new MMapStorage(handler, size, FileChannel.MapMode.READ_WRITE)) {
            for (Midpoint midpoint : midpoints) {
                ByteBuffer data = midpointSerializer.toBytes(midpoint);
                storage.write(data);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write midpoints", e);
        }
    }

    private void load() {
        try (Storage storage = new MMapStorage(handler, handler.length(), FileChannel.MapMode.READ_WRITE)) {
            long pos = 0;
            ByteBuffer data = ByteBuffer.allocate(Midpoint.BYTES);

            while (storage.read(pos, data) > 0) {
                data.flip();
                Midpoint midpoint = midpointSerializer.fromBytes(data);
                midpoints.add(midpoint);
                pos += Midpoint.BYTES;
                data.clear();
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to load midpoints", e);
        }
    }

    private static File getFile(File indexDir, String segmentName) {
        return new File(indexDir, segmentName.split("\\.")[0] + "-MIDPOINT.dat");
    }

    public int getMidpointIdx(IndexEntry entry) {
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

    public Midpoint getMidpointFor(IndexEntry entry) {
        int midpointIdx = getMidpointIdx(entry);
        if (midpointIdx >= midpoints.size() || midpointIdx < 0) {
            return null;
        }
        return midpoints.get(midpointIdx);
    }


    public void delete() {
        try {
            Files.delete(handler.toPath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean inRange(Range range) {
        if(midpoints.isEmpty()) {
            return false;
        }
        return !(range.start().compareTo(last()) > 0 || range.end().compareTo(first()) < 0);
    }

    public int size() {
        return midpoints.size();
    }

    public IndexEntry first() {
        if(midpoints.isEmpty()) {
            return null;
        }
        return firstMidpoint().key;
    }

    public IndexEntry last() {
        if(midpoints.isEmpty()) {
            return null;
        }
        return lastMidpoint().key;
    }

    private Midpoint firstMidpoint() {
        if(midpoints.isEmpty()) {
            return null;
        }
        return midpoints.get(0);
    }

    private Midpoint lastMidpoint() {
        if(midpoints.isEmpty()) {
            return null;
        }
        return midpoints.get(midpoints.size() - 1);
    }


}
