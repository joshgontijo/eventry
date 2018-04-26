package io.joshworks.fstore.es;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class EventStoreIT {

    @Test
    public void insert_1M_same_stream() throws IOException {
        File file = new File("J:\\event-db\\" + UUID.randomUUID().toString().substring(0,8));
        Files.createDirectories(file.toPath());

        EventStore store = EventStore.open(file);

        long start = System.currentTimeMillis();
        int size = 1000000;

        long lastInsert = System.currentTimeMillis();
        for (int i = 0; i < size; i++) {
            store.put("test-stream", Event.create("" + i, "data-" + i));
//            if(i % 1000 == 0) {
//                long now = System.currentTimeMillis();
//                System.out.println("Batch insertion took: " + (now - lastInsert));
//                lastInsert = now;
//            }
        }
        System.out.println("WRITE: " + (System.currentTimeMillis() - start));

        start = System.currentTimeMillis();

        List<Event> events = store.get("test-stream");

        System.out.println("SIZE: " + events.size());
        System.out.println("READ: " + (System.currentTimeMillis() - start));

        store.close();
    }

    @Test
    public void insert_1M_multiple_streams() throws IOException {
        File file = new File("J:\\event-db\\" + UUID.randomUUID().toString().substring(0,8));
        Files.createDirectories(file.toPath());

        EventStore store = EventStore.open(file);

        long start = System.currentTimeMillis();
        int size = 1000000;


        for (int i = 0; i < size; i++) {
            store.put("test-stream-" + i, Event.create("" + i, "data-" + i));
        }
        System.out.println("WRITE: " + (System.currentTimeMillis() - start));

        start = System.currentTimeMillis();

        for (int i = 0; i < size; i++) {
            List<Event> events = store.get("test-stream-" + i);
            assertEquals("Failed on iteration " + i, 1, events.size());
        }

        System.out.println("GET READ: " + (System.currentTimeMillis() - start));


        store.close();
    }

}
