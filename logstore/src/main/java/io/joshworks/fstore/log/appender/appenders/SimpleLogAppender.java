package io.joshworks.fstore.log.appender.appenders;

import io.joshworks.fstore.log.appender.Config;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.log.segment.Segment;

public class SimpleLogAppender<T> extends LogAppender<T, Segment<T>> {

    public SimpleLogAppender(Config<T> config) {
        super(config, Segment::new);
    }

}
