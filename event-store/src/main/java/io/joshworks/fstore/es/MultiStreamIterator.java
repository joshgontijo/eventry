package io.joshworks.fstore.es;

import io.joshworks.fstore.es.index.IndexEntry;
import io.joshworks.fstore.es.log.Event;
import io.joshworks.fstore.es.log.EventLog;
import io.joshworks.fstore.es.utils.Iterators;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

public class MultiStreamIterator implements Iterator<Event> {

    private final Map<Long, String> streamHashMappings;
    private final EventLog eventLog;
    private final Queue<Iterators.PeekingIterator<IndexEntry>> queue;

    public MultiStreamIterator(Map<Long, String> streamHashMappings, Iterable<? extends Iterator<IndexEntry>> iterators, EventLog eventLog) {
        this.streamHashMappings = streamHashMappings;
        this.eventLog = eventLog;
        this.queue = new PriorityQueue<>(2, Comparator.comparingLong(o -> o.peek().position));
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
        IndexEntry next = nextIterator.next();
        if (nextIterator.hasNext()) {
            queue.add(nextIterator);
        }
        String name = streamHashMappings.get(next.stream);
        if(name == null) {
            throw new IllegalStateException("Invalid stream mapping for " + next.stream);
        }
        Event event = eventLog.get(name, next);
        if (event == null) {
            throw new IllegalStateException("Event not found for " + next);
        }
        return event;
    }

}
