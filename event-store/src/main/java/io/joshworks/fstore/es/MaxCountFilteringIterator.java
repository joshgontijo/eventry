package io.joshworks.fstore.es;

import io.joshworks.fstore.es.index.IndexEntry;
import io.joshworks.fstore.log.LogIterator;

import java.io.IOException;
import java.util.NoSuchElementException;

public class MaxCountFilteringIterator implements LogIterator<IndexEntry> {

    private final int maxCount;
    private final int streamVersion;
    private final LogIterator<IndexEntry> delegate;
    private IndexEntry next;

    private MaxCountFilteringIterator(int maxCount, int streamVersion, LogIterator<IndexEntry> delegate) {
        this.maxCount = maxCount;
        this.streamVersion = streamVersion;
        this.delegate = delegate;
        next = dropEvents();
    }

    public static LogIterator<IndexEntry> of(int maxCount, int streamVersion, LogIterator<IndexEntry> delegate) {
        return new MaxCountFilteringIterator(maxCount, streamVersion, delegate);
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public IndexEntry next() {
        if (next == null) {
            throw new NoSuchElementException();
        }
        IndexEntry temp = next;
        next = dropEvents();
        return temp;
    }

    private IndexEntry dropEvents() {
        IndexEntry last;
        do {
            last = nextEntry();
        }while(last != null && lessThanMaxCount(last));
        return last != null && lessThanMaxCount(last) ? null : last;
    }

    private IndexEntry nextEntry() {
        return delegate.hasNext() ? delegate.next() : null;
    }

    private boolean lessThanMaxCount(IndexEntry last) {
        return last.version <= (streamVersion - maxCount);
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