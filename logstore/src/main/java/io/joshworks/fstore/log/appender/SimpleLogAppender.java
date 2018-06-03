package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.log.segment.LogSegment;

public class SimpleLogAppender<T> extends LogAppender<T, LogSegment<T>> {

    protected SimpleLogAppender(Builder<T> builder) {
        super(builder, LogSegment::new);
    }

}
