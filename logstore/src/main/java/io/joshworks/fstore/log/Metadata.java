package io.joshworks.fstore.log;

public class Metadata {
    private long lastPosition;
    private int segmentSize;
    private int segmentBitShift;
    private long maxBlockSize;
    private int blockBitShift;
    private int entryIdxBitShift;

    public static Metadata copy(Metadata from) {
        Metadata metadata = new Metadata();
        metadata.lastPosition = from.lastPosition;
        metadata.segmentSize = from.segmentSize;
        metadata.maxBlockSize = from.maxBlockSize;
        metadata.blockBitShift = from.blockBitShift;
        metadata.entryIdxBitShift = from.entryIdxBitShift;
        metadata.segmentBitShift = from.segmentBitShift;

        return metadata;
    }

    public Metadata lastPosition(final long lastPosition) {
        this.lastPosition = lastPosition;
        return this;
    }

    public Metadata segmentSize(final int segmentSize) {
        this.segmentSize = segmentSize;
        return this;
    }

    public Metadata maxBlockSize(final long maxBlockSize) {
        this.maxBlockSize = maxBlockSize;
        return this;
    }

    public Metadata blockBitShift(final int blockBitShift) {
        this.blockBitShift = blockBitShift;
        return this;
    }

    public Metadata entryIdxBitShift(final int entryIdxBitShift) {
        this.entryIdxBitShift = entryIdxBitShift;
        return this;
    }

    public Metadata segmentBitShift(final int segmentBitShift) {
        this.segmentBitShift = segmentBitShift;
        return this;
    }

    public long lastPosition() {
        return lastPosition;
    }

    public int segmentSize() {
        return segmentSize;
    }

    public int segmentBitShift() {
        return segmentBitShift;
    }

    public long maxBlockSize() {
        return maxBlockSize;
    }

    public int blockBitShift() {
        return blockBitShift;
    }

    public int entryIdxBitShift() {
        return entryIdxBitShift;
    }
}
