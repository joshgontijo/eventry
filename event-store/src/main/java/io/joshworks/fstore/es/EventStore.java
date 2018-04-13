package io.joshworks.fstore.es;

import io.joshworks.fstore.log.appender.Builder;
import io.joshworks.fstore.log.appender.LogAppender;

import java.io.Closeable;
import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class EventStore implements Closeable {

    private final SortedMap<IndexKey, Long> index = new TreeMap<>();

    //ES: LRU map with reading the last version from the index
    private final HashMap<Integer, Integer> streamVersion = new HashMap<>();
    private final LogAppender<Event> log;

    private EventStore(LogAppender<Event> log) {
        this.log = log;

    }

    public static EventStore open(File directory) {
        return new EventStore(LogAppender.simpleLog(new Builder<>(directory, new EventSerializer())));
    }


    public List<Event> get(String stream) {
        return get(stream, 0);
    }

    public List<Event> get(String stream, int versionInclusive) {
        int streamHash = stream.hashCode();
        IndexKey start = new IndexKey(streamHash, versionInclusive, 0);

        IndexKey end = new IndexKey(streamHash, Integer.MAX_VALUE, 0);

        Collection<Long> addresses = index.tailMap(start).headMap(end).values();

        List<Event> events = new LinkedList<>();
        for (Long address : addresses) {
            Event event = log.get(address);
            events.add(event);
        }

        return events;
    }

    public void put(String stream, Event event) {
        long position = log.append(event);
        int streamHash = stream.hashCode();

        int version = streamVersion.compute(streamHash, (k, v) -> v == null ? 0 : ++v);

        //if not available in memory
        int streamSize = index.tailMap(new IndexKey(streamHash, 0, 0)).headMap(new IndexKey(streamHash, Integer.MAX_VALUE, 0)).size();//tODO should position be used here as well, is it possible ?


        IndexKey indexKey = new IndexKey(streamHash, version, position);

        index.put(indexKey, position);
    }

    @Override
    public void close() {
        index.clear();
        log.close();
    }


}
