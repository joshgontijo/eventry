package io.joshworks.fstore.es;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class EventStoreTest {

    private EventStore store;
    private Path testDir;

    @Before
    public void setUp() throws Exception {
        testDir = Files.createTempDirectory(null);
        store = EventStore.open(testDir.toFile());
    }

    @After
    public void tearDown() throws Exception {
        store.close();
        Utils.removeFiles(testDir.toFile());
    }

    @Test
    public void get() {
        //given
        for (int i = 0; i < 10000; i++) {
            store.put("test-" + i, Event.create(String.valueOf(i), "data-" + i));
        }

        //when
        for (int i = 0; i < 10000; i++) {
            List<Event> events = store.get("test-" + i, 0);

            //then
            assertEquals("Found events: " + Arrays.toString(events.toArray(new Event[events.size()])) + " - iteration: " + i, 1, events.size());
            assertEquals("Wrong event data, iteration: " + i, "data-" + i, events.get(0).data);
            assertEquals(String.valueOf(i), events.get(0).type);
        }
    }
    
    @Test
    public void get1() {
    }

    @Test
    public void put() {
    }

    @Test
    public void stream() {
    }

    @Test
    public void close() {
    }
}