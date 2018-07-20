package io.joshworks.fstore.log;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.reader.FixedBufferDataReader;
import io.joshworks.fstore.log.segment.Header;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.LogSegment;
import io.joshworks.fstore.log.segment.Type;
import io.joshworks.fstore.serializer.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class LogSegmentTest {

    private Log<String> segment;
    private File testFile;

    private long FILE_SIZE = 10485760; //10mb

    abstract Storage getStorage(File file, long size);

    private Log<String> create(File theFile) {
        Storage storage = getStorage(theFile, FILE_SIZE);
        return new LogSegment<>(storage, new StringSerializer(), new FixedBufferDataReader(), Type.LOG_HEAD);
    }

    private Log<String> open(File theFile) {
        Storage storage = getStorage(theFile, FILE_SIZE);
        return new LogSegment<>(storage, new StringSerializer(), new FixedBufferDataReader());
    }

    @Before
    public void setUp() {
        testFile = Utils.testFile();
        segment = create(testFile);
    }

    @After
    public void cleanup() {
        IOUtils.closeQuietly(segment);
        Utils.tryDelete(testFile);
    }

    @Test
    public void writePosition() {
        String data = "hello";
        segment.append(data);

        assertEquals(Header.SIZE + 4 + 4 + data.length(), segment.position()); // 4 + 4 (header) + data length
    }

    @Test
    public void writePosition_reopen() throws IOException {
        String data = "hello";
        segment.append(data);

        long position = segment.position();
        segment.close();

        segment = open(testFile);

        assertEquals(position, segment.position());
    }

    @Test
    public void write() {
        String data = "hello";
        segment.append(data);

        LogIterator<String> logIterator = segment.iterator();
        assertTrue(logIterator.hasNext());
        assertEquals(data, logIterator.next());
        assertEquals(Header.SIZE + 4 + 4 + data.length(), logIterator.position()); // 4 + 4 (heading) + data length
    }

    @Test
    public void reader_reopen() throws IOException {
        String data = "hello";
        segment.append(data);

        LogIterator<String> logIterator = segment.iterator();
        assertTrue(logIterator.hasNext());
        assertEquals(data, logIterator.next());

        long position = segment.position();
        segment.close();

        segment = open(testFile);

        logIterator = segment.iterator();
        assertEquals(position, segment.position());
        assertTrue(logIterator.hasNext());
        assertEquals(data, logIterator.next());
        assertEquals(Header.SIZE + Log.ENTRY_HEADER_SIZE + data.length(), logIterator.position()); // 4 + 4 (heading) + data length
    }

    @Test
    public void multiple_readers() {
        String data = "hello";
        segment.append(data);

        LogIterator<String> logIterator1 = segment.iterator();
        assertTrue(logIterator1.hasNext());
        assertEquals(data, logIterator1.next());
        assertEquals(Header.SIZE + Log.ENTRY_HEADER_SIZE  + data.length(), logIterator1.position()); // 4 + 4 (heading) + data length

        LogIterator<String> logIterator2 = segment.iterator();
        assertTrue(logIterator2.hasNext());
        assertEquals(data, logIterator2.next());
        assertEquals(Header.SIZE + Log.ENTRY_HEADER_SIZE  + data.length(), logIterator1.position()); // 4 + 4 (heading) + data length
    }

    @Test
    public void big_entry() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            sb.append(UUID.randomUUID().toString());
        }
        String data = sb.toString();
        segment.append(data);

        LogIterator<String> logIterator1 = segment.iterator();
        assertTrue(logIterator1.hasNext());
        assertEquals(data, logIterator1.next());
        assertEquals(Header.SIZE + Log.ENTRY_HEADER_SIZE + data.length(), logIterator1.position()); // 4 + 4 (heading) + data length

    }

    @Test
    public void get() throws IOException {
        List<Long> positions = new ArrayList<>();

        int items = 10;
        for (int i = 0; i < items; i++) {
            positions.add(segment.append(String.valueOf(i)));
        }
        segment.flush();

        for (int i = 0; i < items; i++) {
            String found = segment.get(positions.get(i));
            assertEquals(String.valueOf(i), found);
        }
    }

    @Test
    public void header_is_stored() throws IOException {
        File file = Utils.testFile();
        Log<String> testSegment = null;
        try {

            testSegment = create(file);
            assertTrue(testSegment.created() > 0);
            assertEquals(0, testSegment.entries());
            assertEquals(0, testSegment.level());
            assertFalse(testSegment.readOnly());

            testSegment.close();

            testSegment = open(file);
            assertTrue(testSegment.created() > 0);
            assertEquals(0, testSegment.entries());
            assertEquals(0, testSegment.level());
            assertFalse(testSegment.readOnly());

            testSegment.append("a");
            testSegment.roll(1);

            testSegment.close();

            assertEquals(1, testSegment.entries());
            assertEquals(1, testSegment.level());
            assertTrue(testSegment.readOnly());

        } finally {
            if(testSegment != null) {
                testSegment.close();
            }
            Utils.tryDelete(file);
        }
    }

    @Test
    public void scanner_0() throws IOException {
        testScanner(0);
    }

    @Test
    public void scanner_1() throws IOException {
        testScanner(1);
    }

    @Test
    public void scanner_10() throws IOException {
        testScanner(10);
    }

    @Test
    public void scanner_1000() throws IOException {
        testScanner(1000);
    }

    private void testScanner(int items) throws IOException {
        List<String> values = new ArrayList<>();
        for (int i = 0; i < items; i++) {
            String value = UUID.randomUUID().toString();
            values.add(value);
            segment.append(value);
        }
        segment.flush();

        int i =0;

        LogIterator<String> logIterator = segment.iterator();
        while(logIterator.hasNext()) {
            assertEquals("Failed on iteration " + i, values.get(i), logIterator.next());
            i++;
        }
        assertEquals(items, i);
    }

    @Test
    public void size() throws IOException {
        segment.append("a");
        segment.append("b");

        assertEquals(Header.SIZE +  (Log.ENTRY_HEADER_SIZE + 1) * 2, segment.size());

        segment.position();
        segment.close();

        segment = open(testFile);
        assertEquals(Header.SIZE + (Log.ENTRY_HEADER_SIZE + 1) * 2, segment.size());
    }

}