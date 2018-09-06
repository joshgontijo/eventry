package io.joshworks.fstore.es;

import io.joshworks.fstore.es.hash.Murmur3Hash;
import io.joshworks.fstore.es.hash.XXHash;
import io.joshworks.fstore.es.index.StreamHasher;
import io.joshworks.fstore.es.index.TableIndex;
import io.joshworks.fstore.es.log.EventRecord;
import io.joshworks.fstore.log.PollingSubscriber;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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
        Utils.tryDelete(new File(directory, "index"));
        Utils.tryDelete(new File(directory, "projections"));
        Utils.tryDelete(directory);
    }

    @Test
    public void write_read() {

        final int size = 1000000;
        long start = System.currentTimeMillis();
        String stream = "test-stream";
        for (int i = 0; i < size; i++) {
            store.append(EventRecord.create(stream, "" + i, "data-" + i));
        }
        System.out.println("WRITE: " + (System.currentTimeMillis() - start));


        Iterator<EventRecord> events = store.fromStreamIter(stream);
        int found = 0;

        start = System.currentTimeMillis();
        while (events.hasNext()) {
            EventRecord next = events.next();
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

        String stream = "test-stream";
        for (int i = 0; i < size; i++) {
            store.append(EventRecord.create(stream, "" + i, "data-" + i));
        }

        System.out.println("WRITE: " + (System.currentTimeMillis() - start));

        start = System.currentTimeMillis();

        Stream<EventRecord> events = store.fromStream(stream);

        System.out.println("SIZE: " + events.count());
        System.out.println("READ: " + (System.currentTimeMillis() - start));

        store.close();
    }

    @Test
    public void insert_1M_unique_streams() {

        long start = System.currentTimeMillis();
        int size = 1000000;

        String streamPrefix = "test-stream-";
        for (int i = 0; i < size; i++) {
            store.append(EventRecord.create(streamPrefix + i, "" + i, "data-" + i));
        }
        System.out.println("WRITE: " + (System.currentTimeMillis() - start));

        start = System.currentTimeMillis();

        for (int i = 0; i < size; i++) {
            Stream<EventRecord> events = store.fromStream(streamPrefix + i);
            assertEquals("Failed on iteration " + i, 1, events.count());
        }

        System.out.println("READ: " + (System.currentTimeMillis() - start));
    }

    @Test
    public void insert_1000000_streams_with_1_version_each() {
        testWith(1000000, 1);
    }

    @Test
    public void fromStream_returns_all_items_when_store_is_reopened() {

        //given
        int size = 10000;
        String streamPrefix = "test-stream-";
        for (int i = 0; i < size; i++) {
            store.append(EventRecord.create(streamPrefix + i, "" + i, "data-" + i));
        }

        store.close();

        try (EventStore store = EventStore.open(directory)) {
            for (int i = 0; i < size; i++) {
                Stream<EventRecord> events = store.fromStream(streamPrefix + i);
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
            store.append(EventRecord.create(streamPrefix + i, "" + i, "data-" + i));
        }

        store.close();

        try (EventStore store = EventStore.open(directory)) {
            Stream<EventRecord> events = store.fromAll();
            assertEquals(size, events.count());
        }
    }

    @Test
    public void get_returns_all_items_when_store_is_reopened() {

        //given
        int size = 10000;
        String streamPrefix = "test-stream-";
        for (int i = 0; i < size; i++) {
            store.append(EventRecord.create(streamPrefix + i, "" + i, "data-" + i));
        }

        store.close();

        try (EventStore store = EventStore.open(directory)) {
            for (int i = 0; i < size; i++) {
                EventRecord event = store.get(streamPrefix + i, 1);
                assertNull(event);
                assertEquals(1, event.version);
                assertEquals(streamPrefix + i, event.stream);
            }
        }
    }

    @Test
    public void events_have_stream_and_version() {
        int numStreams = 1000;
        String streamPrefix = "stream-";
        for (int i = 0; i < numStreams; i++) {
            store.append(EventRecord.create(streamPrefix + i, String.valueOf(i), "data-" + i));
        }

        int version = 0;
        for (int i = 0; i < numStreams; i++) {
            String stream = streamPrefix + i;
            EventRecord event = store.get(streamPrefix + i, version);

            assertNotNull(event);
            assertEquals(stream, event.stream);
            assertEquals(version, event.version);
        }
    }

    @Test
    public void iterator() {
        //given
        int numEvents = 10000;
        String streamPrefix = "test-";
        for (int i = 0; i < numEvents; i++) {
            store.append(EventRecord.create(streamPrefix + i, String.valueOf(i), "data-" + i));
        }

        //when
        for (int i = 0; i < numEvents; i++) {
            Iterator<EventRecord> events = store.fromStreamIter(streamPrefix + i, 1);

            int size = 0;
            while (events.hasNext()) {
                EventRecord event = events.next();
                assertEquals("Wrong event data, iteration: " + i, "data-" + i, new String(event.data, StandardCharsets.UTF_8));
                assertEquals(String.valueOf(i), event.type);
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
        String streamPrefix = "test-";
        for (int stream = 0; stream < numStreams; stream++) {
            for (int version = 1; version <= numVersions; version++) {
                store.append(EventRecord.create(streamPrefix + stream, String.valueOf("type"), "data-" + stream));
            }
        }

        List<String> streams = Arrays.asList("test-0", "test-1", "test-10", "test-100", "test-1000");

        Iterator<EventRecord> eventStream = store.zipStreamsIter(new HashSet<>(streams));

        int eventCounter = 0;
        while (eventStream.hasNext()) {
            EventRecord event = eventStream.next();
            int streamIdx = eventCounter++ / numVersions;
            assertEquals(streams.get(streamIdx), event.stream);
        }

        assertEquals(streams.size() * numVersions, eventCounter);
    }

    @Test
    public void fromAll_return_all_elements_in_insertion_order() {

        int size = 1000;
        for (int i = 0; i < size; i++) {
            store.append(EventRecord.create("stream-" + i, "test", "data"));
        }

        Iterator<EventRecord> it = store.fromAllIter();

        EventRecord last = null;
        int total = 0;
        while (it.hasNext()) {
            EventRecord next = it.next();
            if (last == null)
                last = next;
            assertTrue(next.timestamp >= last.timestamp); //no order is guaranteed for same timestamps
            total++;
        }

        assertEquals(size, total);
    }

    @Test
    public void fromStreams_single_stream() {
        //given
        String streamPrefix = "test-";
        int numStreams = 10000;
        int numVersions = 50;
        for (int stream = 0; stream < numStreams; stream++) {
            for (int version = 0; version < numVersions; version++) {
                store.append(EventRecord.create(streamPrefix + stream, "type", "data-" + stream));
            }
        }

        String eventToQuery = "test-1";
        Iterator<EventRecord> eventStream = store.zipStreamsIter(Set.of(eventToQuery));

        int eventCounter = 0;
        while (eventStream.hasNext()) {
            EventRecord event = eventStream.next();
            assertEquals(eventToQuery, event.stream);
            eventCounter++;
        }

        assertEquals(numVersions, eventCounter);
    }

    @Test
    public void fromStream_returns_data_within_maxCount() {
        //given

        String stream = "test-stream";
        int maxCount = 10;
        int numVersions = 50;
        store.createStream(stream, maxCount, -1);

        for (int version = 0; version < numVersions; version++) {
            store.append(EventRecord.create(stream, "type", "data-" + stream));
        }

        Iterator<EventRecord> eventStream = store.fromStreamIter(stream);

        int eventCounter = 0;
        while (eventStream.hasNext()) {
            EventRecord event = eventStream.next();
            System.out.println(event.version);
            assertTrue(event.version >= numVersions - maxCount);
            eventCounter++;
        }

        assertEquals(maxCount, eventCounter);
    }

    @Test
    public void fromStream_returns_data_within_maxAge() throws InterruptedException {
        //given

        String stream = "test-stream";
        int maxAgeSeconds = 5;
        int numVersions = 50;
        store.createStream(stream, -1, maxAgeSeconds);

        for (int version = 0; version < numVersions; version++) {
            store.append(EventRecord.create(stream, "type", "data-" + stream));
        }

        long count = store.fromStream(stream).count();
        assertEquals(numVersions, count);

        Thread.sleep(maxAgeSeconds * 1000);

        count = store.fromStream(stream).count();
        assertEquals(numVersions, count);
    }

    @Test
    public void linkTo() {
        int size = 1000000;

        System.out.println("Creating entries");
        String streamName = "test-stream";
        for (int i = 0; i < size; i++) {
            store.append(EventRecord.create(streamName, "test", UUID.randomUUID().toString().substring(0, 8)));
        }

        System.out.println("LinkTo 1");
        store.fromStream(streamName).forEach(e -> {
            String firstLetter = Arrays.toString(e.data).substring(0, 1);
            store.linkTo("letter-" + firstLetter, e);
        });

        System.out.println("LinkTo 2");
        store.fromStream(streamName).forEach(e -> {
            String firstLetter = Arrays.toString(e.data).substring(0, 2);
            store.linkTo("letter-" + firstLetter, e);
        });

        System.out.println("LinkTo 3");
        store.fromStream(streamName).forEach(e -> {
            String firstLetter = Arrays.toString(e.data).substring(0, 3);
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
        String streamPrefix = "stream-";
        for (int i = 0; i < size; i++) {
            store.append(EventRecord.create(streamPrefix + i, "test", "data"));
        }

        for (int i = 0; i < size; i++) {
            String stream = streamPrefix + i;
            EventRecord event = store.get(stream, 0);
            assertNotNull(event);
            assertEquals(stream, event.stream);
            assertEquals(0, event.version);
        }
    }

    @Test
    public void many_streams_linkTo() {
        int size = 3000000;
        String allStream = "all";
        for (int i = 0; i < size; i++) {
            store.append(EventRecord.create(allStream, "test", UUID.randomUUID().toString()));
        }

        int numOtherIndexes = 5;

        assertEquals(size, store.fromAll().count());
        assertEquals(size, store.fromStream(allStream).count());

        IntStream.range(0, numOtherIndexes).forEach(i -> {
            long start = System.currentTimeMillis();
            store.fromStream(allStream).forEach(e -> store.linkTo(String.valueOf(i), e));
            System.out.println("LinkTo " + size + " events to stream " + i + " in " + (System.currentTimeMillis() - start));
        });


        assertEquals(size, store.fromStream(allStream).count());

        for (int i = 0; i < numOtherIndexes; i++) {
            long foundSize = store.fromStream(String.valueOf(i)).count();
            assertEquals("Failed on iteration: " + i, size, foundSize);
        }
    }

    @Test
    public void fromStreamsStartingWith_returns_orderedEvents() {
        //given
        int numStreams = 1000;
        int numVersions = 50;
        String streamPrefix = "test-";

        for (int stream = 0; stream < numStreams; stream++) {
            for (int version = 1; version <= numVersions; version++) {
                store.append(EventRecord.create(streamPrefix + stream, String.valueOf("type"), "data-" + stream));
            }
        }

        //some other stream we don't care about
        for (int version = 1; version <= numVersions; version++) {
            store.append(EventRecord.create("someOtherStream", String.valueOf("type"), "data-" + version));
        }

        Iterator<EventRecord> eventStream = store.zipStreamsIter(streamPrefix);

        int eventCounter = 0;
        while (eventStream.hasNext()) {
            EventRecord event = eventStream.next();
            int streamIdx = eventCounter++ / numVersions;
            assertTrue(event.stream.startsWith(streamPrefix));
        }

        assertEquals(numStreams * numVersions, eventCounter);
    }

    @Test
    public void poller_returns_all_items() throws IOException, InterruptedException {

        int items = 1000000;
        String stream = "stream-123";
        for (int i = 0; i < items; i++) {
            store.append(EventRecord.create(stream, "type", "data"));
            if (i % 10000 == 0) {
                System.out.println("WRITE: " + i);
            }
        }

        System.out.println("Write completed");
        try (PollingSubscriber<EventRecord> poller = store.poller(stream)) {
            for (int i = 0; i < items; i++) {
                EventRecord event = poller.take();
                assertNotNull(event);
                assertEquals(i, event.version);
                if (i % 10000 == 0) {
                    System.out.println("READ: " + i);
                }
            }
        }

    }

    @Test
    public void poller_with_multiple_streams_returns_all_items() throws IOException, InterruptedException {

        int items = 1000000;
        String streamPrefix = "stream-";
        Set<String> allStreams = new HashSet<>();
        for (int i = 0; i < items; i++) {
            String stream = streamPrefix + i;
            allStreams.add(stream);
            store.append(EventRecord.create(stream, "type", "data"));
            if (i % 10000 == 0) {
                System.out.println("WRITE: " + i);
            }
        }

        System.out.println("Write completed");
        //Orders of events is not guaranteed across streams
        try (PollingSubscriber<EventRecord> poller = store.poller(allStreams)) {
            for (int i = 0; i < items; i++) {
                EventRecord event = poller.take();
                assertNotNull(event);
                assertEquals("Failed on iteration:" + i + " event:" + event, 0, event.version);


                if (i % 10000 == 0) {
                    System.out.println("READ: " + i);
                }
            }
        }
    }

    @Test
    public void linkTo_events_are_resolved_on_get() {

        var original = "stream-1";
        var linkToStream = "stream-2";
        var created = store.append(EventRecord.create(original, "type", "data"));

        store.linkTo(linkToStream, created);

        EventRecord eventRecord = store.get(linkToStream, 0);
        assertNotNull(eventRecord);
        assertEquals(original, eventRecord.stream);
        assertEquals(0, eventRecord.version);

    }

    @Test
    public void index_is_loaded_with_not_persisted_entries() {

        //given
        String stream = "stream-1";
        //1 segment + in memory
        int size = TableIndex.DEFAULT_FLUSH_THRESHOLD + (TableIndex.DEFAULT_FLUSH_THRESHOLD / 2);
        for (int i = 0; i < size; i++) {
            store.append(EventRecord.create(stream, "type-1", "data-" + 1));
        }

        store.close();

        store = EventStore.open(directory);

        Stream<EventRecord> dataStream = store.fromStream(stream);
        assertEquals(size, dataStream.count());
    }

    private void testWith(int streams, int numVersionPerStream) {
        long start = System.currentTimeMillis();

        String streamPrefix = "test-stream-";

        for (int stream = 0; stream < streams; stream++) {
            for (int version = 1; version <= numVersionPerStream; version++) {
                String streamName = streamPrefix + stream;
                try {
                    store.append(EventRecord.create(streamName, "" + stream, "data-" + stream));
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
                assertEquals(numVersionPerStream - 1, foundVersion);

                //FROM STREAM
                try (Stream<EventRecord> events = store.fromStream(streamName)) {
                    assertEquals(numVersionPerStream, events.collect(Collectors.toList()).size());

                    for (int version = 0; version < numVersionPerStream; version++) {
                        //GET
                        EventRecord event = store.get(streamName, version);
                        assertNotNull(event);
                        assertEquals(streamName, event.stream);
                        assertEquals(version, event.version);
                    }
                }


            } catch (Exception e) {
                throw new RuntimeException("Failed on stream " + streamName, e);
            }
        }

    }

}
