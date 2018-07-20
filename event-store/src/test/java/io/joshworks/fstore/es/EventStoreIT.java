package io.joshworks.fstore.es;

import io.joshworks.fstore.es.hash.Murmur3Hash;
import io.joshworks.fstore.es.hash.XXHash;
import io.joshworks.fstore.es.index.StreamHasher;
import io.joshworks.fstore.es.log.Event;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class EventStoreIT {

    private File directory;
    private EventStore store;

    @Before
    public void setUp() {
        directory = Utils.testFolder();
        store = EventStore.open(directory);
    }

    @After
    public void tearDown() {
        store.close();
//        Utils.tryDelete(new File(directory, "index"));
//        Utils.tryDelete(directory);
    }

    @Test
    public void write_read() {

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

    @Test
    public void insert_1M_same_stream() {

        long start = System.currentTimeMillis();
        int size = 1000000;

        for (int i = 0; i < size; i++) {
            store.add("test-stream", Event.create("" + i, "data-" + i));
        }

        System.out.println("WRITE: " + (System.currentTimeMillis() - start));

        start = System.currentTimeMillis();

        Stream<Event> events = store.fromStream("test-stream");

        System.out.println("SIZE: " + events.count());
        System.out.println("READ: " + (System.currentTimeMillis() - start));

        store.close();
    }

    @Test
    public void insert_1M_unique_streams() {

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

    @Test
    public void insert_1000000_streams_with_1_version_each() {
        testWith(1000000, 1);
    }

    @Test
    public void insert_100000_streams_with_10_version_each() {
        testWith(100000, 10);
    }

    @Test
    public void insert_500000_streams_with_2_version_each() {
        testWith(500000, 2);
    }

    @Test
    public void insert_1000000_streams_with_10_version_each() {
        testWith(1000000, 10);
    }


    @Test
    public void fromStream_returns_all_items_when_store_is_reopened() {

        //given
        int size = 10000;
        String streamPrefix = "test-stream-";
        for (int i = 0; i < size; i++) {
            store.add(streamPrefix + i, Event.create("" + i, "data-" + i));
        }

        store.close();

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
        for (int i = 0; i < size; i++) {
            store.add(streamPrefix + i, Event.create("" + i, "data-" + i));
        }

        store.close();

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
        for (int i = 0; i < size; i++) {
            store.add(streamPrefix + i, Event.create("" + i, "data-" + i));
        }

        store.close();

        try (EventStore store = EventStore.open(directory)) {
            for (int i = 0; i < size; i++) {
                Optional<Event> event = store.get(streamPrefix + i, 1);
                assertTrue(event.isPresent());
                assertEquals(1, event.get().version());
                assertEquals(streamPrefix + i, event.get().stream());
            }
        }
    }

    @Test
    public void events_have_stream_and_version() {
        int numStreams = 1000;
        for (int i = 0; i < numStreams; i++) {
            store.add("stream-" + i, Event.create(String.valueOf(i), "data-" + i));
        }

        for (int i = 0; i < numStreams; i++) {
            String stream = "stream-" + i;
            int version = 1;
            Optional<Event> event = store.get("stream-" + i, version);

            assertTrue(event.isPresent());
            assertEquals(stream, event.get().stream());
            assertEquals(version, event.get().version());
        }
    }

    @Test
    public void iterator() {
        //given
        int numEvents = 10000;
        for (int i = 0; i < numEvents; i++) {
            store.add("test-" + i, Event.create(String.valueOf(i), "data-" + i));
        }

        //when
        for (int i = 0; i < numEvents; i++) {
            Iterator<Event> events = store.iterator("test-" + i, 1);

            int size = 0;
            while (events.hasNext()) {
                Event event = events.next();
                assertEquals("Wrong event data, iteration: " + i, "data-" + i, new String(event.data(), StandardCharsets.UTF_8));
                assertEquals(String.valueOf(i), event.type());
                size++;
            }

            //then
            assertEquals(1, size);
        }
    }

    @Test
    public void fromStreams_return_all_streams_based_on_the_position() {
        //given
        int numStreams = 10000;
        int numVersions = 50;
        for (int stream = 0; stream < numStreams; stream++) {
            for (int version = 1; version <= numVersions; version++) {
                store.add("test-" + stream, Event.create(String.valueOf("type"), "data-" + stream));
            }
        }

        List<String> streams = Arrays.asList("test-0", "test-1", "test-10", "test-100", "test-1000");

        Iterator<Event> eventStream = store.iterateStreams(streams);

        int eventCounter = 0;
        while (eventStream.hasNext()) {
            Event event = eventStream.next();
            int streamIdx = eventCounter++ / numVersions;
            assertEquals(streams.get(streamIdx), event.stream());
        }

        assertEquals(streams.size() * numVersions, eventCounter);
    }

    @Test
    public void fromAll_return_all_elements_in_insertion_order() {

        int size = 1000;
        for (int i = 0; i < size; i++) {
            store.add("stream-" + i, Event.create("test", "data"));
        }

        Iterator<Event> it = store.iterateAll();

        Event last = null;
        int total = 0;
        while (it.hasNext()) {
            Event next = it.next();
            if (last == null)
                last = next;
            assertTrue(next.timestamp() >= last.timestamp()); //no order is guaranteed for same timestamps
            total++;
        }

        assertEquals(size, total);
    }

    @Test
    public void fromStreams_single_stream() {
        //given
        int numStreams = 10000;
        int numVersions = 50;
        for (int stream = 0; stream < numStreams; stream++) {
            for (int version = 1; version <= numVersions; version++) {
                String streamName = "test-" + stream;
                store.add(streamName, Event.create(streamName, "data-" + stream));
            }
        }

        String eventToQuery = "test-1";
        Iterator<Event> eventStream = store.iterateStreams(Arrays.asList(eventToQuery));

        int eventCounter = 0;
        while (eventStream.hasNext()) {
            Event event = eventStream.next();
            assertEquals(eventToQuery, event.type());
            eventCounter++;
        }

        assertEquals(numVersions, eventCounter);
    }

    @Test
    public void linkTo() {
        int size = 1000000;

        System.out.println("Creating entries");
        for (int i = 0; i < size; i++) {
            store.add("test-stream", Event.create("test", UUID.randomUUID().toString().substring(0, 8)));
        }

        System.out.println("LinkTo 1");
        store.fromStream("test-stream").forEach(e -> {
            String firstLetter = Arrays.toString(e.data()).substring(0, 1);
            store.linkTo("letter-" + firstLetter, e);
        });

        System.out.println("LinkTo 2");
        store.fromStream("test-stream").forEach(e -> {
            String firstLetter = Arrays.toString(e.data()).substring(0, 2);
            store.linkTo("letter-" + firstLetter, e);
        });

        System.out.println("LinkTo 3");
        store.fromStream("test-stream").forEach(e -> {
            String firstLetter = Arrays.toString(e.data()).substring(0, 3);
            store.linkTo("letter-" + firstLetter, e);
        });


    }

    //This isn't an actual test, it's more a debugging tool
    @Test
    public void hash_collision() {
        Map<Long, String> hashes = new HashMap<>();
        StreamHasher hasher = new StreamHasher(new XXHash(), new Murmur3Hash());

        for (int i = 0; i < 10000000; i++) {
            String value = "test-stream-" + i;
            long hash = hasher.hash(value);
            if (hashes.containsKey(hash)) {
                fail("Hash collision: " + hashes.get(hash) + " -> " + value);
            }
            hashes.put(hash, value);
        }
    }

    @Test
    public void get() {
        int size = 1000;
        for (int i = 0; i < size; i++) {
            store.add("stream-" + i, Event.create("test", "data"));
        }

        for (int i = 0; i < size; i++) {
            String stream = "stream-" + i;
            Optional<Event> event = store.get(stream, 1);
            assertTrue(event.isPresent());
            assertEquals(stream, event.get().stream());
            assertEquals(1, event.get().version());
        }
    }

    @Test
    public void many_streams_linkTo() {
        int size = 1000000;
        String allStream = "all";
        for (int i = 0; i < size; i++) {
            store.add(allStream, Event.create("test", UUID.randomUUID().toString()));
        }

        int numOtherIndexes = 5;

        long start = System.currentTimeMillis();

        assertEquals(size, store.fromStream(allStream).count());

        store.fromStream(allStream).forEach(e -> {
            for (int i = 0; i < numOtherIndexes; i++) {
                store.linkTo(String.valueOf(i), e);
            }
        });
        System.out.println("Created " + numOtherIndexes + " in " + (System.currentTimeMillis() - start));

        assertEquals(size, store.fromStream(allStream).count());

        for (int i = 0; i < numOtherIndexes; i++) {
            long foundSize = store.fromStream(String.valueOf(i)).count();
            assertEquals("Failed on iteration: " + i, size, foundSize);
        }
    }

    private void testWith(int streams, int numVersionPerStream) {
        long start = System.currentTimeMillis();

        String streamPrefix = "test-stream-";

        for (int stream = 0; stream < streams; stream++) {
            for (int version = 1; version <= numVersionPerStream; version++) {
                String streamName = streamPrefix + stream;
                try {
                    Event event = Event.create("" + stream, "data-" + stream);
                    store.add(streamName, event);
                } catch (Exception e) {
                    throw new RuntimeException("Failed on stream " + streamName, e);
                }
            }
        }
        System.out.println("WRITE: " + (System.currentTimeMillis() - start));


        for (int stream = 0; stream < streams; stream++) {
            String streamName = streamPrefix + stream;
            try {
                //VERSION
                int foundVersion = store.version(streamName);
                assertEquals(numVersionPerStream, foundVersion);

                //FROM STREAM
                Stream<Event> events = store.fromStream(streamName);
                assertEquals(numVersionPerStream, events.count());

                for (int version = 1; version <= numVersionPerStream; version++) {
                    //GET
                    Optional<Event> get = store.get(streamName, version);
                    assertTrue(get.isPresent());
                    assertEquals(streamName, get.get().stream());
                    assertEquals(version, get.get().version());
                }

            } catch (Exception e) {
                throw new RuntimeException("Failed on stream " + streamName, e);
            }
        }

    }

}
