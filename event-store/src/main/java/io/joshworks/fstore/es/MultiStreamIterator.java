package io.joshworks.fstore.es;

import io.joshworks.fstore.core.util.Iterators;
import io.joshworks.fstore.es.index.IndexEntry;
import io.joshworks.fstore.es.log.Event;
import io.joshworks.fstore.es.log.EventLog;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

//Ordered by log position multiple streams
public class MultiStreamIterator implements Iterator<Event> {

    private final Map<Long, String> streamHashMappings;
    private final EventLog eventLog;
    private final Queue<Iterators.PeekingIterator<IndexEntry>> queue;

    MultiStreamIterator(Map<Long, String> streamHashMappings, Iterable<? extends Iterator<IndexEntry>> iterators, EventLog eventLog) {
        this.streamHashMappings = streamHashMappings;
        this.eventLog = eventLog;
        this.queue = new PriorityQueue<>(1000, Comparator.comparingLong(o -> o.peek().position));
        for (Iterator<IndexEntry> iterator : iterators) {
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
    public Event next() {
        Iterators.PeekingIterator<IndexEntry> nextIterator = queue.remove();
        IndexEntry indexEntry = nextIterator.next();
        if (nextIterator.hasNext()) {
            queue.add(nextIterator);
        }
        String stream = streamHashMappings.get(indexEntry.stream);
        if (stream == null) {
            throw new IllegalStateException("Invalid stream mapping for " + indexEntry.stream);
        }
        Event event = eventLog.get(indexEntry.position);
        event.streamInfo(stream, indexEntry);
        return event;
    }

}
