package io.joshworks.fstore.log.appender.it;

import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.log.appender.BlockSegmentBuilder;
import io.joshworks.fstore.log.appender.Builder;
import io.joshworks.fstore.log.appender.LogAppender;

public class BlockCompressesLogAppenderIT extends LogAppenderIT {

    private final Codec codec = new SnappyCodec();

    @Override
    protected LogAppender<String> appender(Builder<String> builder) {
        return LogAppender.blockLog(new BlockSegmentBuilder<>(builder, codec));
    }
}
