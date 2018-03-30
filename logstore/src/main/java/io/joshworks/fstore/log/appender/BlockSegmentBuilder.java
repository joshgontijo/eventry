package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.Codec;

public final class BlockSegmentBuilder<T> {

    final Builder<T> base;
    final Codec codec;
    int entryIdxBitShift = 16;
    int maxBlockSize = 65536; //64kb

    public BlockSegmentBuilder(Builder<T> base, Codec codec) {
        this.base = base;
        this.codec = codec;
    }

    public BlockSegmentBuilder<T> entryBitShift(final int entryIdxBitShift) {
        this.entryIdxBitShift = entryIdxBitShift;
        return this;
    }

    public BlockSegmentBuilder<T> maxBlockSize(int maxBlockSize) {
        this.maxBlockSize = maxBlockSize;
        return this;
    }
}
