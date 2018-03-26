package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.log.Scanner;
import io.joshworks.fstore.log.Utils;
import io.joshworks.fstore.serializer.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class LogAppenderTest {

//    private static final String folder = Files.createTempDirectory(".fstoreTest");
    private LogAppender<String> appender;

    private static final int SEGMENT_SIZE = 1024 * 10;//10kb

    protected abstract LogAppender<String> appender(Builder<String> builder);

    @Before
    public void setUp() throws IOException{
        File testDirectory = Files.createTempDirectory(".fstoreTest").toFile();
        testDirectory.deleteOnExit();
        if (testDirectory.exists()) {
            Utils.tryDelete(testDirectory);
        }

        Builder<String> builder = new Builder<>(testDirectory, new StringSerializer())
                .segmentSize(SEGMENT_SIZE);

        appender = appender(builder);
    }

    @After
    public void cleanup() {
        IOUtils.closeQuietly(appender);
    }

    @Test
    public void roll() throws IOException {
        StringBuilder sb = new StringBuilder();
        while (sb.length() <= SEGMENT_SIZE) {
            sb.append(UUID.randomUUID().toString());
        }
        appender.append(sb.toString());
        appender.append("new-segment");
        assertEquals(2, appender.segments.size());
    }

    @Test
    public void reader() throws IOException {

        StringBuilder sb = new StringBuilder();
        while (sb.length() <= SEGMENT_SIZE) {
            sb.append(UUID.randomUUID().toString());
        }
        appender.append(sb.toString());

        String lastEntry = "FIRST-ENTRY-NEXT-SEGMENT";
        appender.append(lastEntry);
        appender.flush();

        assertEquals(2, appender.segments.size());

        Scanner<String> scanner = appender.scanner();

        String lastValue = null;
        for (String value : scanner) {
            lastValue = value;
        }

        assertEquals(lastEntry, lastValue);
    }

    @Test
    public void positionOnSegment() {

        int segmentIdx = 1;
        long positionOnSegment = 32;
        long position = appender.toSegmentedPosition(segmentIdx, positionOnSegment);

        int segment = appender.getSegment(position);
        long foundPositionOnSegment = appender.getPositionOnSegment(position);

        assertEquals(segmentIdx, segment);
        assertEquals(positionOnSegment, foundPositionOnSegment);
    }

    @Test
    public void get() throws IOException {
        long pos1 = appender.append("1");
        long pos2 = appender.append("2");

        appender.flush();

        assertEquals("1", appender.get(pos1));
        assertEquals("2", appender.get(pos2));
    }

    @Test
    public void position() throws IOException {

        assertEquals(0, appender.position());

        long pos1 = appender.append("1");
        assertEquals(pos1, appender.position());

        long pos2 = appender.append("2");
        assertEquals(pos2, appender.position());

        long pos3 = appender.append("3");
        assertEquals(pos3, appender.position());

        appender.flush();
        Scanner<String> scanner = appender.scanner();

        scanner.hasNext();
        String found = scanner.next();
        assertEquals("1", found);

        assertEquals(pos2, scanner.position());
        scanner.hasNext();
        found = scanner.next();
        assertEquals("2", found);

        assertEquals(pos3, scanner.position());
        scanner.hasNext();
        found = scanner.next();
        assertEquals("3", found);


    }

    @Test
    public void reader_position() throws IOException {

        StringBuilder sb = new StringBuilder();
        while (sb.length() <= SEGMENT_SIZE) {
            sb.append(UUID.randomUUID().toString());
        }

        String lastEntry = "FIRST-ENTRY-NEXT-SEGMENT";
        long lastWrittenPosition = appender.append(lastEntry);

        appender.flush();

        Scanner<String> scanner = appender.scanner(lastWrittenPosition);

        assertTrue(scanner.hasNext());
        assertEquals(lastEntry, scanner.next());
    }

    @Test
    public void segmentBitShift() {

        for (int i = 0; i < appender.maxSegments ; i++) {
            long position = appender.toSegmentedPosition(i, 0);
            assertTrue("Failed on " + i, position >= 0);
        }

    }
}