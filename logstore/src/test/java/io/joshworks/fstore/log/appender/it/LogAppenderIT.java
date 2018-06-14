package io.joshworks.fstore.log.appender.it;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.Utils;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.log.segment.Log;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class LogAppenderIT<L extends Log<String>> {

    private LogAppender<String, L> appender;

    protected abstract LogAppender<String, L> appender(File testDirectory);

    private File testDirectory;

    @Before
    public void setUp() {
        testDirectory = Utils.testFolder();
        testDirectory.deleteOnExit();
        appender = appender(testDirectory);
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

        LogIterator<String> logIterator = appender.scanner();
        assertTrue(logIterator.hasNext());
        assertEquals(data, logIterator.next());

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

        try (LogAppender<String, L> appender = appender(testDirectory)) {
            LogIterator<String> logIterator = appender.scanner();
            assertTrue(logIterator.hasNext());
            assertEquals(data, logIterator.next());
        }
    }

    @Test
    public void insert_scan_1M_2kb_entries() throws InterruptedException {
        int items = 1000000;
        String value = stringOfByteLength(2048);
        appendN(value, items);

        appender.close();

        appender = appender(testDirectory);

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
    public void insert_100_segments_of_1GB_with_2kb_entries() throws InterruptedException {
        String value = stringOfByteLength(2048);

        fillNSegments(value, 99999);
        appender.flush();

        Thread.sleep(120000);
        scanAllAssertingSameValue(value);
    }

    @Test
    public void reopen() {

        appender.close();
        int iterations = 200;

        Long lastPosition = null;
        for (int i = 0; i < iterations; i++) {
            try (LogAppender<String, L> appender = appender(testDirectory)) {
                if (lastPosition != null) {
                    assertEquals(lastPosition, Long.valueOf(appender.position()));
                }
                assertEquals(i, appender.entries());
                appender.append("A");
                lastPosition = appender.position();
            }
        }

        try (LogAppender<String, L> appender = appender(testDirectory)) {
            assertEquals(iterations, appender.stream().count());
            assertEquals(iterations, appender.entries());
        }
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
//            appender.append(value);
            appender.appendAsync(value, pos ->{});
            written++;
        }

        try {
            System.out.println("Waiting....");
            Thread.sleep(120000);
            System.out.println("Waiting completed....");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("APPENDER_WRITE - " + appender.entries() + " IN " + (System.currentTimeMillis() - start) + "ms");
    }

    private void fillNSegments(String value, long numSegments) {
        long start = System.currentTimeMillis();

        long avg = 0;
        long lastUpdate = System.currentTimeMillis();
        long written = 0;

        while (appender.segmentsNames().size() < numSegments) {
            if (System.currentTimeMillis() - lastUpdate >= TimeUnit.SECONDS.toMillis(1)) {
                avg = (avg + written) / 2;
                System.out.println("TOTAL WRITTEN: " + appender.entries() + " - LAST SECOND: " + written + " - AVG: " + avg);
                written = 0;
                lastUpdate = System.currentTimeMillis();

//                if(appender.entries() >= 500000) {
//                        try {
//                            Thread.sleep(60000);
//                        } catch (InterruptedException e) {
//                            e.printStackTrace();
//                        }
//                }

            }
            appender.append(value);
            written++;


        }


        System.out.println("APPENDER_WRITE - " + appender.entries() + " IN " + (System.currentTimeMillis() - start) + "ms");
    }

    private void scanAllAssertingSameValue(String expected) {
        long start = System.currentTimeMillis();
        LogIterator<String> logIterator = appender.scanner();

        long avg = 0;
        long lastUpdate = System.currentTimeMillis();
        long read = 0;
        long totalRead = 0;

        while (logIterator.hasNext()) {
            if (System.currentTimeMillis() - lastUpdate >= TimeUnit.SECONDS.toMillis(1)) {
                avg = (avg + read) / 2;
                System.out.println("TOTAL READ: " + totalRead + " - LAST SECOND: " + read + " - AVG: " + avg);
                read = 0;
                lastUpdate = System.currentTimeMillis();
            }
            String found = logIterator.next();
            assertEquals(expected, found);
            read++;
            totalRead++;
        }

        assertEquals(appender.entries(), totalRead);
        System.out.println("APPENDER_READ -  READ " + totalRead + " ENTRIES IN " + (System.currentTimeMillis() - start) + "ms");
    }
}