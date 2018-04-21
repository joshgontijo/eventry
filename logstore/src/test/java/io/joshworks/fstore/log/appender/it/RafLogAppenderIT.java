package io.joshworks.fstore.log.appender.it;

import io.joshworks.fstore.log.appender.Builder;
import io.joshworks.fstore.log.appender.LogAppender;

public class RafLogAppenderIT extends LogAppenderIT {

    @Override
    protected LogAppender<String> appender(Builder<String> builder) {
        return builder.open();
    }
}
