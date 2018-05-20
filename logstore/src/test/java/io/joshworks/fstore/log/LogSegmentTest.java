package io.joshworks.fstore.log;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.reader.FixedBufferDataReader;
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
import static org.junit.Assert.assertTrue;

public abstract class LogSegmentTest {

    private Log<String> appender;
    private File testFile;

    private long FILE_SIZE = 10485760; //10mb

    abstract Storage getStorage(File file, long size);

    private Log<String> create(File theFile) {
        Storage storage = getStorage(theFile, FILE_SIZE);
        return new LogSegment<>(storage, new StringSerializer(), new FixedBufferDataReader(), 0, false);
    }

    private Log<String> open(long pos) {
        Storage storage = getStorage(testFile, FILE_SIZE);
        return new LogSegment<>(storage, new StringSerializer(), new FixedBufferDataReader(), pos, false);
    }

    @Before
    public void setUp() {
        testFile = Utils.testFile();
        appender = create(testFile);
    }

    @After
    public void cleanup() {
        IOUtils.closeQuietly(appender);
        Utils.tryDelete(testFile);
    }

    @Test
    public void writePosition() {
        String data = "hello";
        appender.append(data);

        assertEquals(4 + 4 + data.length(), appender.position()); // 4 + 4 (header) + data length
    }

    @Test
    public void writePosition_reopen() throws IOException {
        String data = "hello";
        appender.append(data);

        long position = appender.position();
        appender.close();

        appender = open(position);

        assertEquals(4 + 4 + data.length(), appender.position()); // 4 + 4 (heading) + data length
    }

    @Test
    public void write() {
        String data = "hello";
        appender.append(data);

        LogIterator<String> logIterator = appender.iterator();
        assertTrue(logIterator.hasNext());
        assertEquals(data, logIterator.next());
        assertEquals(4 + 4 + data.length(), logIterator.position()); // 4 + 4 (heading) + data length
    }
    
    @Test
    public void reader_reopen() throws IOException {
        String data = "hello";
        appender.append(data);

        LogIterator<String> logIterator = appender.iterator();
        assertTrue(logIterator.hasNext());
        assertEquals(data, logIterator.next());

        long position = appender.position();
        appender.close();

        appender = open(position);

        logIterator = appender.iterator();
        assertTrue(logIterator.hasNext());
        assertEquals(data, logIterator.next());
        assertEquals(4 + 4 + data.length(), logIterator.position()); // 4 + 4 (heading) + data length
    }

    @Test
    public void multiple_readers() {
        String data = "hello";
        appender.append(data);

        LogIterator<String> logIterator1 = appender.iterator();
        assertTrue(logIterator1.hasNext());
        assertEquals(data, logIterator1.next());
        assertEquals(4 + 4 + data.length(), logIterator1.position()); // 4 + 4 (heading) + data length

        LogIterator<String> logIterator2 = appender.iterator();
        assertTrue(logIterator2.hasNext());
        assertEquals(data, logIterator2.next());
        assertEquals(4 + 4 + data.length(), logIterator1.position()); // 4 + 4 (heading) + data length
    }

    @Test
    public void big_entry() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            sb.append(UUID.randomUUID().toString());
        }
        String data = sb.toString();
        appender.append(data);

        LogIterator<String> logIterator1 = appender.iterator();
        assertTrue(logIterator1.hasNext());
        assertEquals(data, logIterator1.next());
        assertEquals(Log.ENTRY_HEADER_SIZE + data.length(), logIterator1.position()); // 4 + 4 (heading) + data length

    }

    @Test
    public void get() throws IOException {
        List<Long> positions = new ArrayList<>();

        int items = 10;
        for (int i = 0; i < items; i++) {
            positions.add(appender.append(String.valueOf(i)));
        }
        appender.flush();

        for (int i = 0; i < items; i++) {
            String found = appender.get(positions.get(i));
            assertEquals(String.valueOf(i), found);
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
            appender.append(value);
        }
        appender.flush();

        int i =0;

        LogIterator<String> logIterator = appender.iterator();
        while(logIterator.hasNext()) {
            assertEquals("Failed on iteration " + i, values.get(i), logIterator.next());
            i++;
        }
        assertEquals(items, i);
    }

    @Test
    public void size() throws IOException {
        appender.append("a");
        appender.append("b");

        assertEquals((Log.ENTRY_HEADER_SIZE + 1) * 2, appender.size());

        long lastPos = appender.position();
        appender.close();

        appender = open(lastPos);
        assertEquals((Log.ENTRY_HEADER_SIZE + 1) * 2, appender.size());
    }

}