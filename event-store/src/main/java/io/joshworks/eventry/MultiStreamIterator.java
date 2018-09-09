package io.joshworks.eventry;

import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.eventry.log.EventLog;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.LogIterator;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;

//Ordered by log position multiple streams
public class MultiStreamIterator implements LogIterator<EventRecord> {

    private final EventLog log;
    private final Queue<Iterators.PeekingIterator<IndexEntry>> queue;

    MultiStreamIterator(Iterable<? extends LogIterator<IndexEntry>> iterators, EventLog log) {
        this.log = log;
        this.queue = new PriorityQueue<>(1000, Comparator.comparingLong(o -> o.peek().position));
        for (LogIterator<IndexEntry> iterator : iterators) {
            if (iterator.hasNext()) {
                queue.add(Iterators.peekingIterator(iterator));
            }
        }
    }

    @Override
    public boolean hasNext() {
        return !queue.isEmpty();
    }

    @Override
    public EventRecord next() {
        Iterators.PeekingIterator<IndexEntry> nextIterator = queue.remove();
        IndexEntry indexEntry = nextIterator.next();
        if (nextIterator.hasNext()) {
            queue.add(nextIterator);
        }
        return log.get(indexEntry.position);
    }

    @Override
    public long position() {
        return 0;
    }

    @Override
    public void close() throws IOException {
        for (Iterators.PeekingIterator<IndexEntry> it : queue) {
            it.close();
        }

    }
}
