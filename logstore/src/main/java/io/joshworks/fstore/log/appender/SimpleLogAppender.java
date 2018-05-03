package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.DataReader;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.LogSegment;

public class SimpleLogAppender<T> extends LogAppender<T, LogSegment<T>> {

    SimpleLogAppender(Builder<T> builder) {
        super(builder);
    }

    @Override
    protected LogSegment<T> createSegment(Storage storage, Serializer<T> serializer, DataReader reader) {
        return new LogSegment<>(storage, serializer, reader, 0, false);
    }

    @Override
    protected LogSegment<T> openSegment(Storage storage, Serializer<T> serializer, DataReader reader, long position, boolean readonly) {
        return new LogSegment<>(storage, serializer, reader, position, readonly);
    }
}
