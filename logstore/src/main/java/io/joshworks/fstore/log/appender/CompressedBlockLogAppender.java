package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.BlockCompressedSegment;
import io.joshworks.fstore.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class CompressedBlockLogAppender<T> extends LogAppender<T> {

    private static final Logger logger = LoggerFactory.getLogger(CompressedBlockLogAppender.class);

    //compressed block log
    private final Codec codec; //may be null
    private final int blockBitShift;
    private final int entryIdxBitShift;
    private final int maxBlockSize;

    CompressedBlockLogAppender(File directory, Serializer<T> serializer, BlockAppenderMetadata metadata, Codec codec) {
        super(directory, serializer, metadata.base);
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
    protected Log<T> openSegment(Storage storage, Serializer<T> serializer, long position, boolean checkIntegrity) {
        return BlockCompressedSegment.open(storage, serializer, codec, position, blockBitShift, entryIdxBitShift);
    }
}
