package io.joshworks.fstore.es.log;

import io.joshworks.fstore.es.index.IndexEntry;
import io.joshworks.fstore.log.appender.Config;
import io.joshworks.fstore.log.appender.appenders.SimpleLogAppender;

public class EventLog extends SimpleLogAppender<Event> {

    public EventLog(Config<Event> config) {
        super(config);
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

}
