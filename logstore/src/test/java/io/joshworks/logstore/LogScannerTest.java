package io.joshworks.logstore;

import io.joshworks.fstore.api.Event;
import io.joshworks.fstore.serializer.json.JsonSerializer;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class LogScannerTest {

    private static final String LOGSTORE_FILE = "logstore.dat";

    private final EventLogStore store = newStore();
    private final LogScanner scanner = new LogScanner(LOGSTORE_FILE);

    @Test
    public void forEach() {
        int size = 10;
        String stream = "abc-123";
        for (int i = 0; i < size; i++) {
            store.save(stream, Event.create(Map.of("k1", "" + i), "TEST-EVENT"));
        }

        List<LogScanner.EventEntry> found = new LinkedList<>();
        scanner.forEach(e -> {
            found.add(e);
            System.out.println(e);
        });

        assertEquals(size, found.size());
    }

    private static EventLogStore newStore() {
        return new EventLogStore(
                new FileLogStore(LOGSTORE_FILE, "rw"),
                new LogScanner(LOGSTORE_FILE),
                new JsonSerializer());
    }
}