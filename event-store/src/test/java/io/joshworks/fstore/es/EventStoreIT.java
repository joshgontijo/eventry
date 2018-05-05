//package io.joshworks.fstore.es;
//
//import io.joshworks.fstore.es.log.Event;
//import org.junit.Test;
//
//import java.io.File;
//import java.io.IOException;
//import java.nio.file.Files;
//import java.util.UUID;
//import java.util.stream.Stream;
//
//import static org.junit.Assert.assertEquals;
//
//public class EventStoreIT {
//
//    @Test
//    public void insert_1M_same_stream() throws IOException {
//        File file = new File("J:\\event-db\\" + UUID.randomUUID().toString().substring(0,8));
//        Files.createDirectories(file.toPath());
//
//        EventStore store = EventStore.open(file);
//
//        long start = System.currentTimeMillis();
//        int size = 1000000;
//
//        long lastInsert = System.currentTimeMillis();
//        for (int i = 0; i < size; i++) {
//            store.add("test-fromStream", Event.open("" + i, "data-" + i));
////            if(i % 1000 == 0) {
////                long now = System.currentTimeMillis();
////                System.out.println("Batch insertion took: " + (now - lastInsert));
////                lastInsert = now;
////            }
//        }
//        System.out.println("WRITE: " + (System.currentTimeMillis() - start));
//
//        start = System.currentTimeMillis();
//
//        Stream<Event> events = store.fromStream("test-fromStream");
//
//        System.out.println("SIZE: " + events.count());
//        System.out.println("READ: " + (System.currentTimeMillis() - start));
//
//        store.close();
//    }
//
//    @Test
//    public void insert_1M_multiple_streams() throws IOException {
//        File file = new File("J:\\event-db\\" + UUID.randomUUID().toString().substring(0,8));
//        Files.createDirectories(file.toPath());
//
//        EventStore store = EventStore.open(file);
//
//        long start = System.currentTimeMillis();
//        int size = 1000000;
//
//
//        for (int i = 0; i < size; i++) {
//            store.add("test-fromStream-" + i, Event.open("" + i, "data-" + i));
//        }
//        System.out.println("WRITE: " + (System.currentTimeMillis() - start));
//
//        start = System.currentTimeMillis();
//
//        for (int i = 0; i < size; i++) {
//            Stream<Event> events = store.fromStream("test-fromStream-" + i);
//            assertEquals("Failed on iteration " + i, 1, events.count());
//        }
//
//        System.out.println("READ: " + (System.currentTimeMillis() - start));
//
//
//        store.close();
//    }
//
//}
