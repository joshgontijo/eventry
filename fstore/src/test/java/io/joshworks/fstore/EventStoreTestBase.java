package io.joshworks.fstore;

import io.joshworks.fstore.event.Event;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class EventStoreTestBase {

    private static final String NAME = "test.dat";
    private EventStore store;

    protected abstract EventStore createStore(String name);

    @Before
    public void setUp(){
        store = createStore(NAME);
    }

    @After
    public void tearDown() {
        store.close();
        File file = new File(NAME);
        if(file.exists())
            file.delete();
    }

    @Test
    public void save() {
        Event event = Event.create("uuid-123", Map.of("k1", "v1"), "EV-TYPE");
        Position position = store.save(event);
        assertEquals(0, position.start);
        assertTrue(position.length > 0);
    }

    @Test
    public void get() {
    }

    private static Event randomEvent() {
        String uuid = UUID.randomUUID().toString();
        String someRand = uuid.substring(0, 8);
        return Event.create(uuid, Map.of("k1", someRand), someRand);
    }
}