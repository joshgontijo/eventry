package io.joshworks.fstore.es;

import io.joshworks.fstore.es.hash.Murmur3Hash;
import io.joshworks.fstore.es.hash.XXHash;
import io.joshworks.fstore.es.index.IndexEntry;
import io.joshworks.fstore.es.index.Range;
import io.joshworks.fstore.es.index.StreamHasher;
import io.joshworks.fstore.es.index.TableIndex;
import io.joshworks.fstore.log.appender.LogAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

public class EventStore implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(EventStore.class);

    private final TableIndex index = new TableIndex();
    private final StreamHasher hasher;

    //ES: LRU map with reading the last version from the index
    private final HashMap<Long, Integer> streamVersion = new HashMap<>();
    private final LogAppender<Event> appender;


    private EventStore(LogAppender<Event> appender) {
        this.appender = appender;
        this.hasher = new StreamHasher(new XXHash(), new Murmur3Hash());
    }

    public static EventStore open(File directory) {
        LogAppender<Event> appender = LogAppender.builder(directory, new EventSerializer()).open();
        return new EventStore(appender);
    }

    public List<Event> get(String stream) {
        return get(stream, 1);
    }

    public List<Event> get(String stream, int versionInclusive) {
        long streamHash = hasher.hash(stream);

        List<IndexEntry> addresses = index.range(Range.of(streamHash, versionInclusive));

        List<Event> events = new ArrayList<>();
        for (IndexEntry address : addresses) {
            Event event = appender.get(address.position);
            if(event == null) {
                throw new IllegalStateException("Event is null");
            }
            events.add(event);
        }

        return events;
    }

    public void put(String stream, Event event) {
        long position = appender.append(event);
        long streamHash = hasher.hash(stream);

        int latestVersion = streamVersion.compute(streamHash, (k, v) -> v == null ? 1 : ++v);

//        int latestVersion = index.latestOfStream(streamHash).map(ie -> ie.version).orElse(1); //TODO change to stream starting at 1

        index.add(streamHash, latestVersion, position);

        if(index.size() >= 500000) {
            String segmentName = appender.currentSegment();
            index.flush(appender.directory().toFile(), segmentName);
            appender.roll();
        }

    }

    public Stream<Event> stream() {
        return appender.stream();
    }

    @Override
    public void close() {
        index.close();
        appender.close();
    }

    public void report() {
        index.report();
    }

}
