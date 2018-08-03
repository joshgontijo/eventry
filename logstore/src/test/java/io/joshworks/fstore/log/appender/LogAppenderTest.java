package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.Mode;
import io.joshworks.fstore.core.io.RafStorage;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.Utils;
import io.joshworks.fstore.log.appender.appenders.SimpleLogAppender;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.LogSegment;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.serializer.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class LogAppenderTest {

    private static final int SEGMENT_SIZE = 1024 * 64;//64kb

    private Config<String> config;
    private LogAppender<String, LogSegment<String>> appender;
    private File testDirectory;

    @Before
    public void setUp() {
        testDirectory = Utils.testFolder();
        testDirectory.deleteOnExit();

        config = LogAppender.builder(testDirectory, new StringSerializer()).segmentSize(SEGMENT_SIZE).disableCompaction();
        appender = new SimpleLogAppender<>(config);
    }

    @After
    public void cleanup() {
        IOUtils.closeQuietly(appender);
        Utils.tryDelete(testDirectory);
    }

    @Test
    public void roll() {
        appender.append("data");
        String firstSegment = appender.currentSegment();

        assertEquals(0, appender.levels.depth());

        appender.roll();

        String secondSegment = appender.currentSegment();
        assertNotEquals(firstSegment, secondSegment);

        assertEquals(2, appender.levels.numSegments());
        assertEquals(2, appender.levels.depth());
        assertEquals(firstSegment, appender.levels.get(0).name());
        assertEquals(secondSegment, appender.levels.get(1).name());
    }

    @Test
    public void roll_size_based() {
        StringBuilder sb = new StringBuilder();
        while (sb.length() <= SEGMENT_SIZE) {
            sb.append(UUID.randomUUID().toString());
        }
        appender.append(sb.toString());
        appender.append("new-segment");

        assertEquals(2, appender.levels.numSegments());
    }

    @Test
    public void scanner_returns_in_insertion_order_with_multiple_segments() {

        appender.append("a");
        appender.append("b");

        appender.roll();

        appender.append("c");
        appender.flush();

        assertEquals(2, appender.levels.numSegments());

        LogIterator<String> logIterator = appender.scanner();

        String lastValue = null;

        while (logIterator.hasNext()) {
            lastValue = logIterator.next();
        }

        assertEquals("c", lastValue);
    }

    @Test
    public void positionOnSegment() {

        int segmentIdx = 0;
        long positionOnSegment = 32;
        long position = appender.toSegmentedPosition(segmentIdx, positionOnSegment);

        int segment = appender.getSegment(position);
        long foundPositionOnSegment = appender.getPositionOnSegment(position);

        assertEquals(segmentIdx, segment);
        assertEquals(positionOnSegment, foundPositionOnSegment);
    }

    @Test
    public void get() {
        long pos1 = appender.append("1");
        long pos2 = appender.append("2");

        appender.flush();

        assertEquals("1", appender.get(pos1));
        assertEquals("2", appender.get(pos2));
    }

    @Test
    public void position() {

        assertEquals(0, appender.position());

        long pos1 = appender.append("1");
        long pos2 = appender.append("2");
        long pos3 = appender.append("3");

        appender.flush();
        LogIterator<String> logIterator = appender.scanner();

        assertEquals(pos1, logIterator.position());
        String found = logIterator.next();
        assertEquals("1", found);

        assertEquals(pos2, logIterator.position());
        found = logIterator.next();
        assertEquals("2", found);

        assertEquals(pos3, logIterator.position());
        found = logIterator.next();
        assertEquals("3", found);
    }

    @Test
    public void reader_position() {

        StringBuilder sb = new StringBuilder();
        while (sb.length() <= SEGMENT_SIZE) {
            sb.append(UUID.randomUUID().toString());
        }

        String lastEntry = "FIRST-ENTRY-NEXT-SEGMENT";
        long lastWrittenPosition = appender.append(lastEntry);

        appender.flush();

        LogIterator<String> logIterator = appender.scanner(lastWrittenPosition);

        assertTrue(logIterator.hasNext());
        assertEquals(lastEntry, logIterator.next());
    }

    @Test
    public void reopen() {

        appender.close();

        long pos1;
        long pos2;
        long pos3;
        long pos4;

        try (LogAppender<String, LogSegment<String>> testAppender = new SimpleLogAppender<>(config)) {
            pos1 = testAppender.append("1");
            pos2 = testAppender.append("2");
            pos3 = testAppender.append("3");
        }
        try (LogAppender<String, LogSegment<String>> testAppender = new SimpleLogAppender<>(config)) {
            pos4 = testAppender.append("4");
        }
        try (LogAppender<String, LogSegment<String>> testAppender = new SimpleLogAppender<>(config)) {
            assertEquals("1", testAppender.get(pos1));
            assertEquals("2", testAppender.get(pos2));
            assertEquals("3", testAppender.get(pos3));
            assertEquals("4", testAppender.get(pos4));

            Set<String> values = testAppender.stream().collect(Collectors.toSet());
            assertTrue(values.contains("1"));
            assertTrue(values.contains("2"));
            assertTrue(values.contains("3"));
            assertTrue(values.contains("4"));
        }
    }

    @Test
    public void when_reopened_use_metadata_instead_builder_params() {
        appender.append("a");
        appender.append("b");

        assertEquals(2, appender.entries());

        appender.close();

        appender = new SimpleLogAppender<>(config);
        assertEquals(2, appender.entries());
        assertEquals(2, appender.stream().count());
    }

    @Test
    public void reopen_brokenEntry() throws IOException {
        appender.close();

        String segmentName;
        try (LogAppender<String, LogSegment<String>> testAppender = new SimpleLogAppender<>(config)) {
            testAppender.append("1");
            testAppender.append("2");
            testAppender.append("3");

            //get last segment (in this case there will be always one)
            segmentName = testAppender.segmentsNames().get(testAppender.segmentsNames().size() - 1);
        }

        //write broken data

        File file = new File(testDirectory, segmentName);
        try (Storage storage = new RafStorage(file, file.length(), Mode.READ_WRITE)) {
            storage.position(Log.START);
            ByteBuffer broken = ByteBuffer.allocate(Log.ENTRY_HEADER_SIZE + 4);
            broken.putInt(444); //expected length
            broken.putInt(123); // broken checksum
            broken.putChar('A'); // broken data
            storage.write(broken);
        }

        try (LogAppender<String, LogSegment<String>> testAppender = new SimpleLogAppender<>(config)) {
            testAppender.append("4");
        }

        try (LogAppender<String, LogSegment<String>> testAppender = new SimpleLogAppender<>(config)) {
            Set<String> values = testAppender.stream().collect(Collectors.toSet());
            assertTrue(values.contains("1"));
            assertTrue(values.contains("2"));
            assertTrue(values.contains("3"));
            assertTrue(values.contains("4"));
        }
    }

    @Test
    public void bad_header_throws_exception() {
        fail("TODO");
    }

    @Test
    public void bad_log_data_is_ignored_when_opening_current_log() {
        fail("TODO");
    }

    @Test
    public void segmentBitShift() {
        for (int i = 0; i < appender.maxSegments; i++) {
            long position = appender.toSegmentedPosition(i, 0);
            long foundSegment = appender.getSegment(position);
            assertEquals("Failed on segIdx " + i + " - position: " + position + " - foundSegment: " + foundSegment, i, foundSegment);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void toSegmentedPosition_invalid() {
        long invalidAddress = appender.maxSegments + 1;
        appender.toSegmentedPosition(invalidAddress, 0);

    }

    @Test
    public void getPositionOnSegment() {

        long value = 1;
        long position = appender.getPositionOnSegment(1);
        assertEquals("Failed on position: " + position, value, position);

        value = appender.maxAddressPerSegment / 2;
        position = appender.getPositionOnSegment(value);
        assertEquals("Failed on position: " + position, value, position);

        value = appender.maxAddressPerSegment;
        position = appender.getPositionOnSegment(value);
        assertEquals("Failed on position: " + position, value, position);
    }

    @Test
    public void reopen_reads_from_segment_header() {

        appender.close();

        //create
        try (LogAppender<String, LogSegment<String>> testAppender = new SimpleLogAppender<>(config)) {
            Log<String> testSegment = testAppender.current();

            assertTrue(testSegment.created() > 0);
            assertEquals(0, testSegment.entries());
            assertEquals(0, testSegment.level());
            assertFalse(testSegment.readOnly());
        }

        //duplicated code, part of the test, do not delete
        //open
        try (LogAppender<String, LogSegment<String>> testAppender = new SimpleLogAppender<>(config)) {
            Log<String> testSegment = testAppender.current();

            assertTrue(testSegment.created() > 0);
            assertEquals(0, testSegment.entries());
            assertEquals(0, testSegment.level());
            assertFalse(testSegment.readOnly());
        }

        //open
        try (LogAppender<String, LogSegment<String>> testAppender = new SimpleLogAppender<>(config)) {
            Log<String> testSegment = testAppender.current();
            testSegment.append("a");
            testSegment.roll(1);

            assertEquals(1, testSegment.entries());
            assertEquals(1, testSegment.level());
            assertTrue(testSegment.readOnly());

        }
    }

    @Test
    public void get_return_all_items() {

        File location = Utils.testFolder();
        try(SimpleLogAppender<String> testAppender = new SimpleLogAppender<>(new Config<>(location, Serializers.STRING).segmentSize(209715200))) {
            List<Long> positions = new ArrayList<>();
            int size = 500000;
            for (int i = 0; i < size; i++) {
                long pos = testAppender.append(String.valueOf(i));
                positions.add(pos);
            }

            for (int i = 0; i < size; i++) {
                String val = testAppender.get(positions.get(i));
                assertEquals(String.valueOf(i), val);
            }
        }



    }

    @Test
    public void compact() {
        appender.append("SEGMENT-A");
        appender.roll();

        appender.append("SEGMENT-B");
        appender.roll();

        assertEquals(3, appender.levels.numSegments());
        assertEquals(2, appender.entries());

        appender.compact();

        assertEquals(2, appender.levels.numSegments());
        assertEquals(3, appender.levels.depth());
        assertEquals(2, appender.entries());

        List<String> found = appender.stream().collect(Collectors.toList());
        assertEquals("SEGMENT-A", found.get(0));
        assertEquals("SEGMENT-B", found.get(1));
    }

    @Test
    @Ignore("Compact is async")
    public void depth_is_correct_after_merge() {

        appender.append("SEGMENT-A");
        appender.roll();

        appender.append("SEGMENT-B");
        appender.roll();

        appender.compact();

        assertEquals(3, appender.depth());
    }

    @Test
    public void iterator_returns_all_elements() {
        int size = 10000;
        int numSegments = 5;

        for (int i = 0; i < size; i++) {
            appender.append(String.valueOf(i));
            if(i > 0 && i % (size / numSegments) == 0) {
                appender.roll();
            }
        }

        assertEquals(size, appender.stream().count());
        assertEquals(size, appender.entries());

        LogIterator<String> scanner = appender.scanner();

        int val = 0;
        while(scanner.hasNext()) {
            String next = scanner.next();
            assertEquals(String.valueOf(val++), next);
        }


    }
}