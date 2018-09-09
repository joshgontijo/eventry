package io.joshworks.eventry.log;

import io.joshworks.eventry.Utils;
import io.joshworks.fstore.log.appender.LogAppender;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;

import java.io.File;

@Ignore
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

    //TODO linkTo no longs uses the entry position

//    @Test
//    public void get_returns_event_with_position() {
//        long pos = eventLog.append(EventRecord.create("stream", "type", "data"));
//        EventRecord event = eventLog.get(pos);
//        assertEquals(pos, event.position);
//    }
//
//    @Test
//    public void scanner_returns_events_with_position() {
//        long pos1 = eventLog.append(EventRecord.create("stream", "type", "data"));
//        long pos2 = eventLog.append(EventRecord.create("stream", "type", "data"));
//
//        LogIterator<EventRecord> scanner = eventLog.scanner();
//        assertEquals(pos1, scanner.next().position());
//        assertEquals(pos2, scanner.next().position());
//    }
//
//    @Test
//    public void stream_returns_events_with_position() {
//
//        long pos1 = eventLog.append(EventRecord.create("stream", "type", "data"));
//        long pos2 = eventLog.append(EventRecord.create("stream", "type", "data"));
//
//        List<Long> positions = eventLog.stream().map(EventRecord::position).collect(Collectors.toList());
//        assertEquals(Long.valueOf(pos1), positions.get(0));
//        assertEquals(Long.valueOf(pos2), positions.get(1));
//    }
}