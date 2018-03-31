package io.joshworks.fstore.log.appender.it;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.log.Scanner;
import io.joshworks.fstore.log.Utils;
import io.joshworks.fstore.log.appender.Builder;
import io.joshworks.fstore.log.appender.LogAppender;
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
        testDirectory = Files.createDirectory(Paths.get(directoryPath)).toFile();
        testDirectory.deleteOnExit();
        if (testDirectory.exists()) {
            Utils.tryDelete(testDirectory);
        }

        Builder<String> builder = new Builder<>(testDirectory, new StringSerializer());

        appender = appender(builder);
    }

    @After
    public void cleanup() {
        IOUtils.closeQuietly(appender);
        Utils.tryDelete(testDirectory);
    }

    @Test
    public void insert_get_1M() throws IOException {
        int items = 1000000;
        String value = "A";

        appendN(value, items);
        appender.flush();

        scanAllAssertingSameValue(value);
    }

    @Test
    public void insert_scan_1M_2kb_entries() throws IOException {
        int items = 1000000;
        String value = stringOfByteLength(2048);
        appendN(value, items);

        scanAllAssertingSameValue(value);
    }

    @Test
    public void insert_1_segments_of_1GB_with_2kb_entries() throws IOException {
        String value = stringOfByteLength(2048);

        fillNSegments(value, 1);
        appender.flush();

        scanAllAssertingSameValue(value);
    }

    @Test
    public void insert_10_segments_of_1GB_with_2kb_entries() throws IOException {
        String value = stringOfByteLength(2048);

        fillNSegments(value, 10);
        appender.flush();

        scanAllAssertingSameValue(value);
    }

    @Test
    public void insert_100_segments_of_1GB_with_2kb_entries() throws IOException {
        String value = stringOfByteLength(2048);

        fillNSegments(value, 100);
        appender.flush();
        scanAllAssertingSameValue(value);
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
            if(System.currentTimeMillis() - lastUpdate >= TimeUnit.SECONDS.toMillis(1)) {
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

        while(appender.segments().size() < numSegments) {
            if(System.currentTimeMillis() - lastUpdate >= TimeUnit.SECONDS.toMillis(1)) {
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
            if(System.currentTimeMillis() - lastUpdate >= TimeUnit.SECONDS.toMillis(1)) {
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
        System.out.println("APPENDER_READ -  READ " + read + " ENTRIES IN "+  (System.currentTimeMillis() - start) + "ms");
    }
}