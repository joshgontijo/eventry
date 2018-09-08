package io.joshworks.fstore.es;

import io.joshworks.fstore.es.log.EventRecord;
import io.joshworks.fstore.log.LogIterator;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;

public class MaxAgeFilteringIterator implements LogIterator<EventRecord> {

    private final LogIterator<EventRecord> delegate;
    private EventRecord next;
    private final long timestamp = System.currentTimeMillis();
    private Map<String, Long> maxAges;

    public MaxAgeFilteringIterator(Map<String, Long> metadataMap, LogIterator<EventRecord> delegate) {
        this.maxAges = metadataMap;
        this.delegate = delegate;
        next = dropEvents();
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public EventRecord next() {
        if (next == null) {
            throw new NoSuchElementException();
        }
        EventRecord temp = next;
        next = dropEvents();
        return temp;
    }

    private EventRecord dropEvents() {
        EventRecord last = nextEntry();
        while(last != null && !withinMaxAge(last)) {
            last = nextEntry();
        }
        return last != null && withinMaxAge(last) ? last : null;
    }

    private boolean withinMaxAge(EventRecord event) {
        Long maxAge = maxAges.get(event.stream);
        return maxAge <= 0 || ((timestamp - event.timestamp) / 1000) <= maxAge;
    }

    private EventRecord nextEntry() {
        return delegate.hasNext() ? delegate.next() : null;
    }

    @Override
    public long position() {
        return delegate.position();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}