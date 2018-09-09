package io.joshworks.eventry;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.log.LogIterator;

import java.io.IOException;
import java.util.function.Function;

public class LinkToResolveIterator implements LogIterator<EventRecord> {

    private final LogIterator<EventRecord> delegate;
    private final Function<EventRecord, EventRecord> resolver;

    public LinkToResolveIterator(LogIterator<EventRecord> delegate, Function<EventRecord, EventRecord> resolver) {
        this.delegate = delegate;
        this.resolver = resolver;
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    @Override
    public EventRecord next() {
        EventRecord next = delegate.next();
        if (next == null) {
            return null;
        }
        return resolver.apply(next);
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
