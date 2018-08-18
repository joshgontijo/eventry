package io.joshworks.fstore.es;

import io.joshworks.fstore.es.index.IndexEntry;
import io.joshworks.fstore.es.log.Event;
import io.joshworks.fstore.es.log.EventLog;
import io.joshworks.fstore.log.LogIterator;

import java.io.IOException;

public class SingleStreamIterator implements LogIterator<Event> {

    private final String stream;
    private final LogIterator<IndexEntry> indexIterator;
    private final EventLog eventLog;

    public SingleStreamIterator(String stream, LogIterator<IndexEntry> indexIterator, EventLog eventLog) {
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
        Event event = eventLog.get(indexEntry.position);
        event.streamInfo(stream, indexEntry);
        return event;
    }

    @Override
    public long position() {
        return indexIterator.position();
    }

    @Override
    public void close() throws IOException {
        indexIterator.close();
    }
}
