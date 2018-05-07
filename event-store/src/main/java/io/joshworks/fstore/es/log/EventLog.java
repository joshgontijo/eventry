package io.joshworks.fstore.es.log;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.DataReader;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.es.index.IndexEntry;
import io.joshworks.fstore.log.LogSegment;
import io.joshworks.fstore.log.appender.Builder;
import io.joshworks.fstore.log.appender.LogAppender;

public class EventLog extends LogAppender<Event, LogSegment<Event>> {

    public EventLog(Builder<Event> builder) {
        super(builder);
    }

    public Event get(String stream, IndexEntry key) {
        Event event = get(key.position);
        if(event == null) {
            throw new IllegalArgumentException("No event found for " + key);
        }
        event.stream(stream);
        event.version(key.version);
        event.position(key.position);

        return event;
    }

    @Override
    protected LogSegment<Event> createSegment(Storage storage, Serializer<Event> serializer, DataReader reader) {
        return new LogSegment<>(storage, serializer, reader, 0, false);
    }

    @Override
    protected LogSegment<Event> openSegment(Storage storage, Serializer<Event> serializer, DataReader reader, long position, boolean readonly) {
        return new LogSegment<>(storage, serializer, reader, position, readonly);
    }

}
