//package io.joshworks.fstore.es;
//
//import io.joshworks.fstore.es.log.Event;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Test;
//
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.util.Arrays;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Optional;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertTrue;
//
//public class EventStoreTest {
//
//    private EventStore store;
//    private Path testDir;
//
//    @Before
//    public void setUp() throws Exception {
//        testDir = Files.createTempDirectory(null);
//        store = EventStore.open(testDir.toFile());
//    }
//
//    @After
//    public void tearDown() throws Exception {
//        store.close();
//        Utils.removeFiles(testDir.toFile());
//    }
//
//    @Test
//    public void events_have_stream_and_version() {
//        int numStreams = 1000;
//        for (int i = 0; i < numStreams; i++) {
//            store.add("stream-" + i, Event.open(String.valueOf(i), "data-" + i));
//        }
//
//        for (int i = 0; i < numStreams; i++) {
//            String stream = "stream-" + i;
//            int version = 1;
//            Optional<Event> event = store.get("stream-" + i, version);
//
//            assertTrue(event.isPresent());
//            assertEquals(stream, event.get().stream());
//            assertEquals(version, event.get().version());
//        }
//    }
//
//    @Test
//    public void iterator() {
//        //given
//        int numEvents = 10000;
//        for (int i = 0; i < numEvents; i++) {
//            store.add("test-" + i, Event.open(String.valueOf(i), "data-" + i));
//        }
//
//        //when
//        for (int i = 0; i < numEvents; i++) {
//            Iterator<Event> events = store.iterator("test-" + i, 0);
//
//            int size = 0;
//            while (events.hasNext()) {
//                Event event = events.next();
//                assertEquals("Wrong event data, iteration: " + i, "data-" + i, event.data());
//                assertEquals(String.valueOf(i), event.type());
//                size++;
//            }
//
//            //then
//            assertEquals(1, size);
//        }
//    }
//
//    @Test
//    public void fromStreams_return_all_streams_based_on_the_position() {
//        //given
//        int numStreams = 10000;
//        int numVersions = 50;
//        for (int stream = 0; stream < numStreams; stream++) {
//            for (int version = 1; version <= numVersions; version++) {
//                store.add("test-" + stream, Event.open(String.valueOf("type"), "data-" + stream));
//            }
//        }
//
//        List<String> streams = Arrays.asList("test-0", "test-1", "test-10", "test-100", "test-1000");
//
//        Iterator<Event> eventStream = store.iterateStreams(streams);
//
//        int eventCounter = 0;
//        while(eventStream.hasNext()) {
//            Event event = eventStream.next();
//            int streamIdx = eventCounter++ / numVersions;
//            assertEquals(streams.get(streamIdx), event.stream());
//        }
//
//        assertEquals(streams.size() * numVersions, eventCounter);
//    }
//
//    @Test
//    public void fromAll_return_all_elements_in_insertion_order() {
//
//        int size = 1000;
//        for (int i = 0; i < size; i++) {
//            store.add("stream-" + i, Event.open("test", "data"));
//        }
//
//        Iterator<Event> it = store.iterateAll();
//
//        Event last = null;
//        int total = 0;
//        while(it.hasNext()) {
//            Event next = it.next();
//            if(last == null)
//                last = next;
//            assertTrue(next.timestamp() >= last.timestamp()); //no order is guaranteed for same timestamps
//            total++;
//        }
//
//        assertEquals(size, total);
//    }
//
//    @Test
//    public void fromStreams_single_stream() {
//        //given
//        int numStreams = 10000;
//        int numVersions = 50;
//        for (int stream = 0; stream < numStreams; stream++) {
//            for (int version = 1; version <= numVersions; version++) {
//                String streamName = "test-" + stream;
//                store.add(streamName, Event.open(streamName, "data-" + stream));
//            }
//        }
//
//        String eventToQuery = "test-1";
//        Iterator<Event> eventStream = store.iterateStreams(Arrays.asList(eventToQuery));
//
//        int eventCounter = 0;
//        while(eventStream.hasNext()) {
//            Event event = eventStream.next();
//            assertEquals(eventToQuery, event.type());
//            eventCounter++;
//        }
//
//        assertEquals(numVersions, eventCounter);
//    }
//
//    @Test
//    public void get() {
//        int size = 1000;
//        for (int i = 0; i < size; i++) {
//            store.add("stream-" + i, Event.open("test", "data"));
//        }
//
//        for (int i = 0; i < size; i++) {
//            String stream = "stream-" + i;
//            Optional<Event> event = store.get(stream, 1);
//            assertTrue(event.isPresent());
//            assertEquals(stream, event.get().stream());
//            assertEquals(1, event.get().version());
//        }
//    }
//
//}