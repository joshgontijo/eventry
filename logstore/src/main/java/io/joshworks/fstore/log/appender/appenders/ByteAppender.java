package io.joshworks.fstore.log.appender.appenders;

import io.joshworks.fstore.log.appender.Config;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.log.appender.SegmentFactory;
import io.joshworks.fstore.log.segment.Log;

//TODO implement me
public class ByteAppender<L extends Log<Put>> extends LogAppender<Put, L> {
    protected ByteAppender(Config<Put> config, SegmentFactory<Put, L> factory) {
        super(config, factory);
    }
}
