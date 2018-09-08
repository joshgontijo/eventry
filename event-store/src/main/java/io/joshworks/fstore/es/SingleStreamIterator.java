package io.joshworks.fstore.es;

import io.joshworks.fstore.es.index.IndexEntry;
import io.joshworks.fstore.es.log.EventLog;
import io.joshworks.fstore.es.log.EventRecord;
import io.joshworks.fstore.log.LogIterator;

import java.io.IOException;

public class SingleStreamIterator implements LogIterator<EventRecord> {

    private final LogIterator<IndexEntry> indexIterator;
    private final EventLog log;

    public SingleStreamIterator(LogIterator<IndexEntry> indexIterator, EventLog log) {
        this.indexIterator = indexIterator;
        this.log = log;
    }

    @Override
    public boolean hasNext() {
        return indexIterator.hasNext();
    }

    @Override
    public EventRecord next() {
        IndexEntry indexEntry = indexIterator.next();
        return log.get(indexEntry.position);
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
