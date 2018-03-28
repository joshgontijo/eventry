package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.Codec;

public class BlockCompressesLogAppenderTest extends LogAppenderTest {

    private final Codec codec = new SnappyCodec();

    @Override
    protected LogAppender<String> appender(Builder<String> builder) {
        return LogAppender.blockLog(new BlockSegmentBuilder<>(builder, codec));
    }
}
