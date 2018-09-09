package io.joshworks.eventry.log;

import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.PollingSubscriber;
import io.joshworks.fstore.log.appender.Config;
import io.joshworks.fstore.log.appender.appenders.SimpleLogAppender;

import java.io.IOException;
import java.util.stream.Stream;

public class EventLog {

    private final SimpleLogAppender<EventRecord> appender;

    public EventLog(Config<EventRecord> config) {
        this.appender = new SimpleLogAppender<>(config);
    }

    public long append(EventRecord event) {
        return appender.append(event);
    }

    public EventRecord get(long position) {
        EventRecord event = appender.get(position);
        if (event == null) {
            throw new IllegalArgumentException("No event found for " + position);
        }
        return event;
    }

    public long size() {
        return appender.entries();
    }

    public long position() {
        return appender.position();
    }

    public void close() {
        appender.close();
    }

    public LogIterator<EventRecord> iterator(Direction direction) {
        return new EventLogIterator(appender.iterator(direction));
    }

    public Stream<EventRecord> stream(Direction direction) {
        return Iterators.stream(iterator(direction));
    }

    public PollingSubscriber<EventRecord> poller() {
        return appender.poller();
    }

    public PollingSubscriber<EventRecord> poller(long position) {
        return appender.poller(position);
    }

    private static class EventLogIterator implements LogIterator<EventRecord> {

        private final LogIterator<EventRecord> iterator;

        private EventLogIterator(LogIterator<EventRecord> iterator) {
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
        public EventRecord next() {
            return iterator.next();
        }
    }

}
