package io.joshworks.fstore.es;

import io.joshworks.fstore.es.index.IndexEntry;
import io.joshworks.fstore.es.log.Event;
import io.joshworks.fstore.es.log.EventLog;

import java.util.Iterator;

public class SingleStreamIterator implements Iterator<Event> {

    private final String stream;
    private final Iterator<IndexEntry> indexIterator;
    private final EventLog eventLog;

    public SingleStreamIterator(String stream, Iterator<IndexEntry> indexIterator, EventLog eventLog) {
        this.stream = stream;
        this.indexIterator = indexIterator;
        this.eventLog = eventLog;
    }

    @Override
    public boolean hasNext() {
        return indexIterator.hasNext();
    }

    @Override
    public Event next() {
        IndexEntry indexEntry = indexIterator.next();
        Event event = eventLog.get(stream, indexEntry);
        if(event == null) {
            throw new IllegalStateException("Event not found for " + indexEntry);
        }
        return event;
    }
}
