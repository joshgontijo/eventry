package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.Mode;
import io.joshworks.fstore.core.io.RafStorage;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.LogFileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public class Metadata {

    static final int METADATA_SIZE = (Integer.BYTES * 3) + (Byte.BYTES * 2);

    final int segmentSize;
    final int segmentBitShift;
    final int maxSegmentsPerLevel;
    final boolean mmap;
    final boolean asyncFlush;

    private Metadata(int segmentSize, int segmentBitShift, int maxSegmentsPerLevel, boolean mmap, boolean asyncFlush) {
        this.segmentSize = segmentSize;
        this.segmentBitShift = segmentBitShift;
        this.mmap = mmap;
        this.asyncFlush = asyncFlush;
        this.maxSegmentsPerLevel = maxSegmentsPerLevel;
    }

    public static Metadata readFrom(File directory) {
        try (Storage storage = new RafStorage(new File(directory, LogFileUtils.METADATA_FILE), METADATA_SIZE, Mode.READ_WRITE)) {
            ByteBuffer bb = ByteBuffer.allocate(METADATA_SIZE);
            storage.read(0, bb);
            bb.flip();

            int segmentSize = bb.getInt();
            int segmentBitShift = bb.getInt();
            int maxSegmentsPerLevel = bb.getInt();
            boolean mmap = bb.get() == 1;
            boolean asyncFlush = bb.get() == 1;

            return new Metadata(segmentSize, segmentBitShift, maxSegmentsPerLevel, mmap, asyncFlush);
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    public static Metadata create(File directory, int segmentSize, int segmentBitShift, int maxSegmentsPerLevel, boolean mmap, boolean asyncFlush) {
        try (Storage storage = new RafStorage(new File(directory, LogFileUtils.METADATA_FILE), METADATA_SIZE, Mode.READ_WRITE)) {
            ByteBuffer bb = ByteBuffer.allocate(METADATA_SIZE);
            bb.putInt(segmentSize);
            bb.putInt(segmentBitShift);
            bb.putInt(maxSegmentsPerLevel);
            bb.put(mmap ? (byte) 1 : 0);
            bb.put(asyncFlush ? (byte) 1 : 0);

            bb.flip();
            storage.write(bb);
            return new Metadata(segmentSize, segmentBitShift, maxSegmentsPerLevel, mmap, asyncFlush);
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }

    }


}
