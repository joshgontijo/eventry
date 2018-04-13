package io.joshworks.fstore.es;

import io.joshworks.fstore.log.appender.Builder;
import io.joshworks.fstore.log.appender.LogAppender;

import java.io.Closeable;
import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class EventStore implements Closeable {

    private final SortedMap<Long, Long> index = new TreeMap<>();
    private final LogAppender<Event> log;

    private EventStore(LogAppender<Event> log) {
        this.log = log;

    }

    public static EventStore open(File directory) {
        return new EventStore(LogAppender.simpleLog(new Builder<>(directory, new EventSerializer())));
    }


    public static void main(String[] args) {
        EventStore store = EventStore.open(new File("event-db"));
        store.put("yolo", new Event("yolo", "yolo1"));
        store.put("yolo", new Event("yolo", "yolo2"));
        store.put("josh", new Event("yolo", "josh1"));
        store.put("josh", new Event("yolo", "josh2"));

        List<Event> yolos = store.get("yolo");
        System.out.println(Arrays.toString(yolos.toArray(new Event[yolos.size()])));

        List<Event> joshs = store.get("josh");
        System.out.println(Arrays.toString(joshs.toArray(new Event[joshs.size()])));

        store.close();

    }


    public List<Event> get(String stream) {
        long streamHash = stream.hashCode();
        long start =  streamHash << 32;

        long mask = (1L << 32) - 1L;
        long end = (start | mask);

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
        long streamHash = stream.hashCode();


        long address = (streamHash << 32) | position;
        System.out.println(streamHash + " --> " + address);

        index.put(address, position);
    }

    @Override
    public void close() {
        index.clear();
        log.close();
    }
}
