package io.joshworks.fstore.log.appender.it;

import io.joshworks.fstore.log.appender.Builder;
import io.joshworks.fstore.log.appender.SimpleLogAppender;

public class RafLogAppenderIT extends LogAppenderIT {

    @Override
    protected SimpleLogAppender<String> appender(Builder<String> builder) {
        return builder.open();
    }
}
