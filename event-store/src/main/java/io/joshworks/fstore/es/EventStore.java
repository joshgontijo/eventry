package io.joshworks.fstore.es;

import io.joshworks.fstore.es.hash.Murmur3Hash;
import io.joshworks.fstore.es.hash.XXHash;
import io.joshworks.fstore.es.index.IndexEntry;
import io.joshworks.fstore.es.index.Range;
import io.joshworks.fstore.es.index.StreamHasher;
import io.joshworks.fstore.es.index.TableIndex;
import io.joshworks.fstore.es.log.Event;
import io.joshworks.fstore.es.log.EventLog;
import io.joshworks.fstore.es.log.EventSerializer;
import io.joshworks.fstore.log.appender.LogAppender;

import java.io.Closeable;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class EventStore implements Closeable {

    private final TableIndex index = new TableIndex();
    private final StreamHasher hasher;

    //ES: LRU map with reading the last version from the index
    private final HashMap<Long, Integer> streamVersion = new HashMap<>();
    private final EventLog eventLog;


    private EventStore(LogAppender<Event> appender) {
        this.eventLog = new EventLog(appender);
        this.hasher = new StreamHasher(new XXHash(), new Murmur3Hash());
    }

    public static EventStore open(File directory) {
        LogAppender<Event> appender = LogAppender.builder(directory, new EventSerializer()).open();
        return new EventStore(appender);
    }

    public Iterator<Event> iterator(String stream) {
        return iterator(stream, 1);
    }

    public Iterator<Event> iterator(String stream, int versionInclusive) {
        long streamHash = hasher.hash(stream);
        Iterator<IndexEntry> addresses = index.iterator(Range.of(streamHash, versionInclusive));
        return new SingleStreamIterator(stream, addresses, eventLog);
    }

    public Stream<Event> fromStreams(List<String> streams) {
        Iterator<Event> iterator = iterateStreams(streams);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false);
    }

    public Iterator<Event> iterateStreams(List<String> streams) {
        Set<String> uniqueStreams = new LinkedHashSet<>(streams);

        List<Iterator<IndexEntry>> indexes = new ArrayList<>(streams.size());
        Map<Long, String> mappings = new HashMap<>();
        for (String stream : uniqueStreams) {
            if(stream == null || stream.isEmpty()) {
                throw new IllegalArgumentException("Stream cannot empty");
            }
            long streamHash = hasher.hash(stream);
            Iterator<IndexEntry> indexStream = index.iterator(Range.allOf(streamHash));

            indexes.add(indexStream);
            mappings.put(streamHash, stream);
        }

        return new MultiStreamIterator(mappings, indexes, eventLog);
    }

    public Stream<Event> fromStream(String stream) {
        return fromStream(stream, 1);
    }

    public Stream<Event> fromStream(String stream, int versionInclusive) {
        Iterator<Event> iterator = iterator(stream, versionInclusive);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false);
    }

    public Iterator<Event> iterateAll() {
        return eventLog.iterator();
    }

    public Stream<Event> fromAll() {
        return eventLog.stream();
    }

    public Optional<Event> get(String stream, int version) {
        if(version <= 0) {
            throw new IllegalArgumentException("Version must be greater than zero");
        }
        long streamHash = hasher.hash(stream);
        Range range = Range.of(streamHash, version, version + 1);
        return index.stream(range).map(i -> eventLog.get(stream, i)).findFirst();
    }

    public void add(String stream, Event event) {
        add(stream, event, 0);
    }

    public void add(String stream, Event event, int expectedVersion) {
        long streamHash = hasher.hash(stream);

//        int currentVersion = streamVersion.compute(streamHash, (k, v) -> v == null ? 1 : ++v);
        int currentVersion = index.version(streamHash);

        if(expectedVersion > 0 && currentVersion != expectedVersion) {
            //TODO return result, no need for exception here
        }

        long position = eventLog.add(event);
        index.add(streamHash, currentVersion + 1, position);

        if(index.inMemoryItems() >= 5000000) {
            roll();
        }
    }

    public void roll() {
        String rolledSegmentName = eventLog.roll();
        index.flush(eventLog.directory(), rolledSegmentName);
    }

    @Override
    public void close() {
        index.close();
        eventLog.close();
    }
}