package io.joshworks.fstore.log.appender.it;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.log.Scanner;
import io.joshworks.fstore.log.Utils;
import io.joshworks.fstore.log.appender.Builder;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.serializer.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class LogAppenderIT {

    private LogAppender<String> appender;

    protected abstract LogAppender<String> appender(Builder<String> builder);

    protected File testDirectory;
    private static String directoryPath;


    @BeforeClass
    public static void init() {
        System.setProperty("fstore.dir", "J:\\FSTORE2");

        String fromProps = System.getProperty("fstore.dir");
        String fromEnv = System.getenv("FSTORE_DIR");

        if (fromProps == null && fromEnv == null) {
            throw new IllegalStateException("System property 'fstore.dir' or env variable 'FSTORE_DIR' must be provided");
        }
        directoryPath = fromProps == null ? fromEnv : fromProps;
    }

    @Before
    public void setUp() throws IOException {
        testDirectory = new File(directoryPath);
        if (testDirectory.exists()) {
            Utils.tryDelete(testDirectory);
        }
        testDirectory = Files.createDirectory(Paths.get(directoryPath)).toFile();
        testDirectory.deleteOnExit();

        Builder<String> builder = LogAppender.builder(testDirectory, new StringSerializer());

        appender = appender(builder);
    }

    @After
    public void cleanup() {
        IOUtils.closeQuietly(appender);
        Utils.tryDelete(testDirectory);
    }

    @Test
    public void insert_get_1M() {
        int items = 1000000;
        String value = "A";

        appendN(value, items);
        appender.flush();

        scanAllAssertingSameValue(value);
    }

    @Test
    public void shrink() {

        String data = "DATA";
        appender.append(data);
        long lastPos = appender.position();
        String name = appender.currentSegment();
        appender.roll();

        File f = new File(testDirectory, name);
        if (!Files.exists(f.toPath())) {
            fail("File " + f + " doesn't exist");
        }

        assertEquals(lastPos, f.length()); //header + data + EOF;

        Scanner<String> scanner = appender.scanner();
        assertTrue(scanner.hasNext());
        assertEquals(data, scanner.next());

    }

    @Test
    public void reopening_after_shrinking_returns_all_data() {

        String data = "DATA";
        long position;
        String name;


        appender.append(data);
        position = appender.position();
        name = appender.currentSegment();
        appender.roll();

        appender.close();

        File f = new File(testDirectory, name);
        if (!Files.exists(f.toPath())) {
            fail("File " + f + " doesn't exist");
        }

        assertEquals(position, f.length());

        try (LogAppender<String> appender = appender(LogAppender.builder(testDirectory, Serializers.STRING))) {
            Scanner<String> scanner = appender.scanner();
            assertTrue(scanner.hasNext());
            assertEquals(data, scanner.next());
        }
    }

    @Test
    public void insert_scan_1M_2kb_entries() {
        int items = 1000000;
        String value = stringOfByteLength(2048);
        appendN(value, items);

        appender.flush();

        scanAllAssertingSameValue(value);
    }

    @Test
    public void insert_1_segments_of_1GB_with_2kb_entries() {
        String value = stringOfByteLength(2048);

        fillNSegments(value, 1);
        appender.flush();

        scanAllAssertingSameValue(value);
    }

    @Test
    public void insert_10_segments_with_2kb_entries() {
        String value = stringOfByteLength(2048);

        fillNSegments(value, 10);
        appender.flush();

        scanAllAssertingSameValue(value);
    }

    @Test
    public void insert_100_segments_of_1GB_with_2kb_entries() {
        String value = stringOfByteLength(2048);

        fillNSegments(value, 100);
        appender.flush();
        scanAllAssertingSameValue(value);
    }

    @Test
    public void reopen() {

        appender.close();
        int iterations = 200;

        Long lastPosition = null;
        for (int i = 0; i < iterations; i++) {
            try (LogAppender<String> appender = appender(LogAppender.builder(testDirectory, Serializers.STRING))) {
                if (lastPosition != null) {
                    assertEquals(lastPosition, Long.valueOf(appender.position()));
                }
                assertEquals(i, appender.entries());
                appender.append("A");
                lastPosition = appender.position();
            }
        }

        try (LogAppender<String> appender = appender(LogAppender.builder(testDirectory, Serializers.STRING))) {
            assertEquals(iterations, appender.stream().count());
            assertEquals(iterations, appender.entries());
        }
    }

    @Test(expected = IllegalStateException.class)
    public void deleting_the_current_segment_must_throw_exception() {

        String name = appender.currentSegment();
        appender.delete(name);

        assertEquals(1, appender.segments().size());
        assertNotEquals(name, appender.currentSegment());


    }

    private static String stringOfByteLength(int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length / Character.BYTES; i++)
            sb.append("A");
        return sb.toString();
    }

    private void appendN(String value, long num) {
        long start = System.currentTimeMillis();

        long avg = 0;
        long lastUpdate = System.currentTimeMillis();
        long written = 0;

        for (int i = 0; i < num; i++) {
            if (System.currentTimeMillis() - lastUpdate >= TimeUnit.SECONDS.toMillis(1)) {
                avg = (avg + written) / 2;
                System.out.println("TOTAL WRITTEN: " + appender.entries() + " - LAST SECOND: " + written + " - AVG: " + avg);
                written = 0;
                lastUpdate = System.currentTimeMillis();
            }
            appender.append(value);
            written++;
        }

        System.out.println("APPENDER_WRITE - " + appender.entries() + " IN " + (System.currentTimeMillis() - start) + "ms");
    }

    private void fillNSegments(String value, long numSegments) {
        long start = System.currentTimeMillis();

        long avg = 0;
        long lastUpdate = System.currentTimeMillis();
        long written = 0;

        while (appender.segments().size() < numSegments) {
            if (System.currentTimeMillis() - lastUpdate >= TimeUnit.SECONDS.toMillis(1)) {
                avg = (avg + written) / 2;
                System.out.println("TOTAL WRITTEN: " + appender.entries() + " - LAST SECOND: " + written + " - AVG: " + avg);
                written = 0;
                lastUpdate = System.currentTimeMillis();
            }
            appender.append(value);
            written++;
        }


        System.out.println("APPENDER_WRITE - " + appender.entries() + " IN " + (System.currentTimeMillis() - start) + "ms");
    }

    private void scanAllAssertingSameValue(String expected) {
        long start = System.currentTimeMillis();
        Scanner<String> scanner = appender.scanner();

        long avg = 0;
        long lastUpdate = System.currentTimeMillis();
        long read = 0;
        long totalRead = 0;

        while (scanner.hasNext()) {
            if (System.currentTimeMillis() - lastUpdate >= TimeUnit.SECONDS.toMillis(1)) {
                avg = (avg + read) / 2;
                System.out.println("TOTAL READ: " + totalRead + " - LAST SECOND: " + read + " - AVG: " + avg);
                read = 0;
                lastUpdate = System.currentTimeMillis();
            }
            String found = scanner.next();
            assertEquals(expected, found);
            read++;
            totalRead++;
        }

        assertEquals(appender.entries(), totalRead);
        System.out.println("APPENDER_READ -  READ " + read + " ENTRIES IN " + (System.currentTimeMillis() - start) + "ms");
    }
}