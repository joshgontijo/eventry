package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.BlockCompressedSegment;
import io.joshworks.fstore.log.Log;

import java.io.File;

public class CompressedBlockLogAppender<T> extends LogAppender<T> {

    //compressed block log
    private final Codec codec; //may be null
    private final int blockBitShift;
    private final int entryIdxBitShift;
    private final int maxBlockSize;

    CompressedBlockLogAppender(File directory, Serializer<T> serializer, BlockAppenderMetadata metadata, State state, Codec codec) {
        super(directory, serializer, metadata.base, state);
        this.codec = codec;
        this.blockBitShift = metadata.blockBitShift;
        this.entryIdxBitShift = metadata.entryIdxBitShift;
        this.maxBlockSize = metadata.maxBlockSize;
    }

    @Override
    protected Log<T> createSegment(Storage storage, Serializer<T> serializer) {
        return BlockCompressedSegment.create(storage, serializer, codec, maxBlockSize, blockBitShift, entryIdxBitShift);
    }

    @Override
    protected Log<T> openSegment(Storage storage, Serializer<T> serializer, long position) {
        return BlockCompressedSegment.open(storage, serializer, codec, maxBlockSize, position, blockBitShift, entryIdxBitShift);
    }
}
