package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.Codec;

public final class BlockSegmentBuilder<T> {

    final Builder<T> base;
    final Codec codec;
    int blockBitShift = 26;
    int entryIdxBitShift = 10;
    int maxBlockSize = 64000; //64kb

    public BlockSegmentBuilder(Builder<T> base, Codec codec) {
        this.base = base;
        this.codec = codec;
    }

    public BlockSegmentBuilder<T> blockBitShift(final int blockBitShift) {
        this.blockBitShift = blockBitShift;
        return this;
    }

    public BlockSegmentBuilder<T> entryBitShift(final int entryIdxBitShift) {
        this.entryIdxBitShift = entryIdxBitShift;
        return this;
    }

    public BlockSegmentBuilder maxBlockSize(int maxBlockSize) {
        this.maxBlockSize = maxBlockSize;
        return this;
    }
}
