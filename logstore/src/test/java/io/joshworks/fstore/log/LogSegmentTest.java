package io.joshworks.fstore.log;

import io.joshworks.fstore.core.io.DiskStorage;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.serializer.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class LogSegmentTest {

    private LogSegment<String> appender;
    private Path testFile;
    private Storage storage;


    abstract Storage getStorage(File file);

    @Before
    public void setUp() throws IOException {
        testFile = Files.createTempFile(UUID.randomUUID().toString().substring(0, 8), ".dat");
        storage = getStorage(testFile.toFile());
        appender = LogSegment.create(storage, new StringSerializer());
    }

    @After
    public void cleanup() {
        IOUtils.closeQuietly(storage);
        IOUtils.closeQuietly(appender);
        Utils.tryDelete(testFile.toFile());
    }

    @Test
    public void writePosition() {
        String data = "hello";
        appender.append(data);

        assertEquals(4 + 4 + data.length(), appender.position()); // 4 + 4 (heading) + data length
    }

    @Test
    public void writePosition_reopen() {
        String data = "hello";
        appender.append(data);

        long position = appender.position();
        appender.close();

        appender = LogSegment.open(storage, new StringSerializer(), position);

        assertEquals(4 + 4 + data.length(), appender.position()); // 4 + 4 (heading) + data length
    }

    @Test
    public void write() {
        String data = "hello";
        appender.append(data);

        Scanner<String> scanner = appender.scanner();
        assertTrue(scanner.hasNext());
        assertEquals(data, scanner.next());
        assertEquals(4 + 4 + data.length(), scanner.position()); // 4 + 4 (heading) + data length
    }
    
    @Test
    public void reader_reopen() {
        String data = "hello";
        appender.append(data);

        Scanner<String> scanner = appender.scanner();
        assertTrue(scanner.hasNext());
        assertEquals(data, scanner.next());

        long position = appender.position();
        appender.close();

        storage = new DiskStorage(testFile.toFile());
        appender = LogSegment.open(storage, new StringSerializer(), position);

        scanner = appender.scanner();
        assertTrue(scanner.hasNext());
        assertEquals(data, scanner.next());
        assertEquals(4 + 4 + data.length(), scanner.position()); // 4 + 4 (heading) + data length
    }

    @Test
    public void multiple_readers() {
        String data = "hello";
        appender.append(data);

        Scanner<String> scanner1 = appender.scanner();
        assertTrue(scanner1.hasNext());
        assertEquals(data, scanner1.next());
        assertEquals(4 + 4 + data.length(), scanner1.position()); // 4 + 4 (heading) + data length

        Scanner<String> scanner2 = appender.scanner();
        assertTrue(scanner2.hasNext());
        assertEquals(data, scanner2.next());
        assertEquals(4 + 4 + data.length(), scanner1.position()); // 4 + 4 (heading) + data length
    }

    @Test
    public void big_entry() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            sb.append(UUID.randomUUID().toString());
        }
        String data = sb.toString();
        appender.append(data);

        Scanner<String> scanner1 = appender.scanner();
        assertTrue(scanner1.hasNext());
        assertEquals(data, scanner1.next());
        assertEquals(Log.HEADER_SIZE + data.length(), scanner1.position()); // 4 + 4 (heading) + data length

    }

    @Test
    public void get() {
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
    public void getWithLength() {
        List<Long> positions = new ArrayList<>();
        List<String> values = new ArrayList<>();

        int items = 10;
        for (int i = 0; i < items; i++) {
            String value = UUID.randomUUID().toString();
            values.add(value);
            positions.add(appender.append(value));
        }
        appender.flush();

        int length = 36;
        for (int i = 0; i < items; i++) {
            String found = appender.get(positions.get(i), length);
            assertEquals(values.get(i), found);
        }
    }

    @Test
    public void scanner_0() {
        testScanner(0);
    }

    @Test
    public void scanner_1() {
        testScanner(1);
    }

    @Test
    public void scanner_10() {
        testScanner(10);
    }

    @Test
    public void scanner_1000() {
        testScanner(1000);
    }

    private void testScanner(int items) {
        List<String> values = new ArrayList<>();
        for (int i = 0; i < items; i++) {
            String value = UUID.randomUUID().toString();
            values.add(value);
            appender.append(value);
        }
        appender.flush();

        int i =0;
        for (String s : appender.scanner()) {
            assertEquals("Failed on iteration " + i, values.get(i), s);
            i++;
        }
        assertEquals(items, i);
    }
}