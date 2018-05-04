package io.joshworks.fstore.log.appender.it;

import io.joshworks.fstore.log.appender.Builder;
import io.joshworks.fstore.log.appender.SimpleLogAppender;

public class MmapSimpleLogAppenderIT extends LogAppenderIT {

    @Override
    protected SimpleLogAppender<String> appender(Builder<String> builder) {
        return builder.mmap().open();
    }
}
