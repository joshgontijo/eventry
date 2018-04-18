package io.joshworks.fstore.es;

import io.joshworks.fstore.es.hash.Murmur3;
import io.joshworks.fstore.es.hash.XXHash;
import io.joshworks.fstore.es.index.IndexEntry;
import io.joshworks.fstore.es.index.IndexHasher;
import io.joshworks.fstore.es.index.Range;
import io.joshworks.fstore.es.index.TableIndex;
import io.joshworks.fstore.log.appender.Builder;
import io.joshworks.fstore.log.appender.LogAppender;

import java.io.Closeable;
import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public class EventStore implements Closeable {

    private final TableIndex index = new TableIndex();
    private final IndexHasher hasher;

    //ES: LRU map with reading the last version from the index
    private final HashMap<Long, Integer> streamVersion = new HashMap<>();
    private final LogAppender<Event> appender;


    private EventStore(LogAppender<Event> appender) {
        this.appender = appender;
        this.hasher = new IndexHasher(new XXHash(), new Murmur3());
    }

    public static EventStore open(File directory) {
        return new EventStore(LogAppender.simpleLog(new Builder<>(directory, new EventSerializer()).mmap()));
    }

    public List<Event> get(String stream) {
        return get(stream, 0);
    }

    public List<Event> get(String stream, int versionInclusive) {
        long streamHash = hasher.hash(stream);

        Set<IndexEntry> addresses = index.range(Range.of(streamHash, versionInclusive));

        List<Event> events = new LinkedList<>();
        for (IndexEntry address : addresses) {
            Event event = appender.get(address.position);
            events.add(event);
        }

        return events;
    }

    public void put(String stream, Event event) {
        long position = appender.append(event);
        long streamHash = hasher.hash(stream);

//        int version = streamVersion.compute(streamHash, (k, v) -> v == null ? 0 : ++v);
//        int streamSize = index.range(Range.allOf(streamHash)).size();//TODO should position be used here as well, is it possible ?

        int latestVersion = index.lastOfStream(streamHash).map(ie -> ie.version).orElse(-1); //TODO change to stream starting at 1

        index.add(streamHash, latestVersion + 1, position);

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
//        index.clear();
        appender.close();
    }


}
