package io.joshworks.fstore.es.log;

import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.PollingSubscriber;
import io.joshworks.fstore.log.appender.Config;
import io.joshworks.fstore.log.appender.appenders.SimpleLogAppender;

import java.io.IOException;
import java.util.stream.Stream;

public class EventLog {

    private final SimpleLogAppender<Event> appender;

    public EventLog(Config<Event> config) {
        this.appender = new SimpleLogAppender<>(config);
    }

    public long append(Event event) {
        return appender.append(event);
    }

    public Event get(long position) {
        Event event = appender.get(position);
        if (event == null) {
            throw new IllegalArgumentException("No event found for " + position);
        }
        event.position(position);
        return event;
    }

    public long size() {
        return appender.entries();
    }

    public void close() {
        appender.close();
    }

    public LogIterator<Event> scanner() {
        return new EventLogIterator(appender.scanner());
    }

    public Stream<Event> stream() {
        return Iterators.stream(scanner());
    }

    public PollingSubscriber<Event> poller() {
        return appender.poller();
    }

    public PollingSubscriber<Event> poller(long position) {
        return appender.poller(position);
    }

    private static class EventLogIterator implements LogIterator<Event> {

        private final LogIterator<Event> iterator;

        private EventLogIterator(LogIterator<Event> iterator) {
            this.iterator = iterator;
        }

        @Override
        public long position() {
            return iterator.position();
        }

        @Override
        public void close() throws IOException {
            iterator.close();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Event next() {
            long position = iterator.position();
            Event next = iterator.next();
            next.position(position);
            return next;
        }
    }

}
