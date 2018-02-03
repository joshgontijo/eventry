package io.joshworks.logstore;

import io.joshworks.fstore.api.Event;
import io.joshworks.fstore.api.EventStream;
import io.joshworks.fstore.serializer.json.JsonSerializer;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class EventLogStoreTest {

    private static final String LOGSTORE_FILE = "logstore.dat";

    private EventLogStore logStore = newStore();

    @After
    public void tearDown() throws IOException {
        logStore.close();
        Files.delete(new File(LOGSTORE_FILE).toPath());
    }

    @Test
    public void save() {
        Event created = logStore.save("stream-123", Event.create(Map.of("k1", "v1"), "TEST-EVENT"));
        System.out.println(created);

        assertNotNull(created.getUuid());

        Event found = logStore.get(created.getUuid());
        assertEquals(created, found);
    }

    @Test
    public void getStream() {
        int size = 10;
        String stream = "abc-123";
        for (int i = 0; i < size; i++) {
            logStore.save(stream, Event.create(Map.of("k1", i), "TEST-EVENT"));
        }

        EventStream found = logStore.getStream(stream);
        assertNotNull(found);
        assertEquals(size, found.size());
    }

    @Test
    public void rebuildIndex() {
        int size = 10;
        String stream = "abc-123";
        for (int i = 0; i < size; i++) {
            logStore.save(stream, Event.create(Map.of("k1", i), "TEST-EVENT"));
        }

        logStore.close();
        logStore = newStore();

        EventStream events = EventStream.empty();
        logStore.forEach(events::add);

        assertEquals(size, events.size());
    }


    private static EventLogStore newStore() {
        return new EventLogStore(
                new FileLogStore(LOGSTORE_FILE, "rw"),
                new LogScanner(LOGSTORE_FILE),
                new JsonSerializer());
    }
}