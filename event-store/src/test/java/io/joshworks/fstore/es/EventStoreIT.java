package io.joshworks.fstore.es;

import io.joshworks.fstore.es.log.Event;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EventStoreIT {

    private File directory;

    @Before
    public void setUp() throws Exception {
        directory = new File("J:\\event-db\\" + UUID.randomUUID().toString().substring(0, 8));
        Files.createDirectories(directory.toPath());
    }

    @After
    public void tearDown() throws Exception {
        Utils.tryDelete(new File(directory, "index"));
        Utils.tryDelete(directory);
    }

    @Test
    public void write_read() {

        try (EventStore store = EventStore.open(directory)) {
            final int size = 1000000;
            long start = System.currentTimeMillis();
            for (int i = 0; i < size; i++) {
                store.add("test-stream", Event.create("" + i, "data-" + i));
            }
            System.out.println("WRITE: " + (System.currentTimeMillis() - start));


            Iterator<Event> events = store.iterator("test-stream");
            int found = 0;

            start = System.currentTimeMillis();
            while (events.hasNext()) {
                Event next = events.next();
                found++;
            }
            System.out.println("READ: " + (System.currentTimeMillis() - start));

            if (found != size) {
                throw new RuntimeException("Expected " + size + " Got " + found);
            }
        }
    }

    @Test
    public void insert_1M_same_stream() {

        EventStore store = EventStore.open(directory);

        long start = System.currentTimeMillis();
        int size = 1000000;

        long lastInsert = System.currentTimeMillis();
        for (int i = 0; i < size; i++) {
            store.add("test-stream", Event.create("" + i, "data-" + i));
//            if(i % 1000 == 0) {
//                long now = System.currentTimeMillis();
//                System.out.println("Batch insertion took: " + (now - lastInsert));
//                lastInsert = now;
//            }
        }
        System.out.println("WRITE: " + (System.currentTimeMillis() - start));

        start = System.currentTimeMillis();

        Stream<Event> events = store.fromStream("test-stream");

        System.out.println("SIZE: " + events.count());
        System.out.println("READ: " + (System.currentTimeMillis() - start));

        store.close();
    }

    @Test
    public void insert_1M_multiple_streams() {

        try (EventStore store = EventStore.open(directory)) {
            long start = System.currentTimeMillis();
            int size = 1000000;

            for (int i = 0; i < size; i++) {
                store.add("test-stream-" + i, Event.create("" + i, "data-" + i));
            }
            System.out.println("WRITE: " + (System.currentTimeMillis() - start));

            start = System.currentTimeMillis();

            for (int i = 0; i < size; i++) {
                Stream<Event> events = store.fromStream("test-stream-" + i);
                assertEquals("Failed on iteration " + i, 1, events.count());
            }

            System.out.println("READ: " + (System.currentTimeMillis() - start));
        }
    }

    @Test
    public void fromStream_returns_all_items_when_store_is_reopened() {

        //given
        int size = 10000;
        String streamPrefix = "test-stream-";
        try (EventStore store = EventStore.open(directory)) {
            for (int i = 0; i < size; i++) {
                store.add(streamPrefix + i, Event.create("" + i, "data-" + i));
            }
        }
        try (EventStore store = EventStore.open(directory)) {
            for (int i = 0; i < size; i++) {
                Stream<Event> events = store.fromStream(streamPrefix + i);
                assertEquals("Failed on iteration " + i, 1, events.count());
            }
        }
    }

    @Test
    public void fromAll_returns_all_items_when_store_is_reopened() {

        //given
        int size = 10000;
        String streamPrefix = "test-stream-";
        try (EventStore store = EventStore.open(directory)) {
            for (int i = 0; i < size; i++) {
                store.add(streamPrefix + i, Event.create("" + i, "data-" + i));
            }
        }
        try (EventStore store = EventStore.open(directory)) {
            Stream<Event> events = store.fromAll();
            assertEquals(size, events.count());
        }
    }

    @Test
    public void get_returns_all_items_when_store_is_reopened() {

        //given
        int size = 10000;
        String streamPrefix = "test-stream-";
        try (EventStore store = EventStore.open(directory)) {
            for (int i = 0; i < size; i++) {
                store.add(streamPrefix + i, Event.create("" + i, "data-" + i));
            }
        }
        try (EventStore store = EventStore.open(directory)) {
            for (int i = 0; i < size; i++) {
                Optional<Event> event = store.get(streamPrefix + i, 1);
                assertTrue(event.isPresent());
                assertEquals(1, event.get().version());
                assertEquals(streamPrefix + i, event.get().stream());
            }
        }
    }

}
