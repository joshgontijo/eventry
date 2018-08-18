package io.joshworks.fstore.es.log;

import io.joshworks.fstore.es.Utils;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.LogAppender;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class EventLogTest {

    private EventLog eventLog;
    private File testDir;

    @Before
    public void setUp() {
        testDir = Utils.testFolder();
        eventLog = new EventLog(LogAppender.builder(testDir, new EventSerializer()));
    }

    @After
    public void tearDown() {
        eventLog.close();
        Utils.tryDelete(testDir);
    }

    @Test
    public void append_adds_sequence() {
        long pos = eventLog.append(Event.create("stream", "type", "data"));
        Event event = eventLog.get(pos);
        assertEquals(0, event.sequence());
    }

    @Test
    public void reopening_maintains_correct_sequence() {
        eventLog.append(Event.create("stream", "type", "data"));
        eventLog.append(Event.create("stream", "type", "data"));
        eventLog.close();

        eventLog = new EventLog(LogAppender.builder(testDir, new EventSerializer()));
        assertEquals(2, eventLog.size());

        long last = eventLog.append(Event.create("stream", "type", "data"));
        assertEquals(3, eventLog.size());

        Event event = eventLog.get(last);
        assertEquals(2, event.sequence());

    }

    @Test
    public void get_returns_event_with_position() {
        long pos = eventLog.append(Event.create("stream", "type", "data"));
        Event event = eventLog.get(pos);
        assertEquals(pos, event.position());
    }

    @Test
    public void events_are_stored_with_sequence() {
        long pos1 = eventLog.append(Event.create("stream", "type", "data"));
        long pos2 = eventLog.append(Event.create("stream", "type", "data"));

        Event event1 = eventLog.get(pos1);
        Event event2 = eventLog.get(pos2);
        assertEquals(0, event1.sequence());
        assertEquals(1, event2.sequence());
    }

    @Test
    public void scanner_returns_events_with_position() {
        long pos1 = eventLog.append(Event.create("stream", "type", "data"));
        long pos2 = eventLog.append(Event.create("stream", "type", "data"));

        LogIterator<Event> scanner = eventLog.scanner();
        assertEquals(pos1, scanner.next().position());
        assertEquals(pos2, scanner.next().position());
    }

    @Test
    public void stream_returns_events_with_position() {

        long pos1 = eventLog.append(Event.create("stream", "type", "data"));
        long pos2 = eventLog.append(Event.create("stream", "type", "data"));

        List<Long> positions = eventLog.stream().map(Event::position).collect(Collectors.toList());
        assertEquals(Long.valueOf(pos1), positions.get(0));
        assertEquals(Long.valueOf(pos2), positions.get(1));
    }
}