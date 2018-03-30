package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.BitUtil;
import io.joshworks.fstore.log.BlockCompressedSegment;
import io.joshworks.fstore.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class BlockCompressedLogAppender<T> extends LogAppender<T> {

    private static final Logger logger = LoggerFactory.getLogger(BlockCompressedLogAppender.class);

    //compressed block log
    private final Codec codec; //may be null

    private final int entryIdxBitShift;
    private final int segmentBitShift;

    private final int maxBlockSize;
    private final long maxBlockAddressPerSegment;
    private final long maxEntriesPerBlock;

    BlockCompressedLogAppender(File directory, Serializer<T> serializer, BlockAppenderMetadata metadata, State state, Codec codec) {
        super(directory, serializer, metadata.base, state);
        this.codec = codec;
        this.entryIdxBitShift = metadata.entryIdxBitShift;
        this.maxBlockSize = metadata.maxBlockSize;
        this.segmentBitShift = metadata.base.segmentBitShift;

        if (metadata.entryIdxBitShift >= metadata.base.segmentBitShift || metadata.entryIdxBitShift < 0) {
            //just a numeric validation, values near 'segmentBitShift' and 0 are still nonsense
            throw new IllegalArgumentException("entryIdxBitShift must be between 0 and " + (metadata.base.segmentBitShift - 1) + " (segmentBitShift)");
        }

        this.maxBlockAddressPerSegment = BitUtil.maxValueForBits(metadata.base.segmentBitShift - metadata.entryIdxBitShift);
        this.maxEntriesPerBlock = BitUtil.maxValueForBits(metadata.entryIdxBitShift);

        logger.info("MAX BLOCK ADDRESS PER SEGMENT: {} ({} bits)", maxBlockAddressPerSegment, metadata.base.segmentBitShift - metadata.entryIdxBitShift);
        logger.info("MAX ENTRIES PER BLOCK: {} ({} bits)", maxEntriesPerBlock, metadata.entryIdxBitShift);
    }

    @Override
    protected Log<T> createSegment(Storage storage, Serializer<T> serializer) {
        return BlockCompressedSegment.create(storage, serializer, codec, maxBlockSize, maxBlockAddressPerSegment, entryIdxBitShift);
    }

    @Override
    protected Log<T> openSegment(Storage storage, Serializer<T> serializer, long position) {
        return BlockCompressedSegment.open(storage, serializer, codec, maxBlockSize, maxBlockAddressPerSegment, entryIdxBitShift, position);
    }
}
