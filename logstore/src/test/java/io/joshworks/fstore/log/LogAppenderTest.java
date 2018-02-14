package io.joshworks.fstore.log;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.serializer.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LogAppenderTest {

    private static final String folder = System.getProperty("user.home") + File.separator + ".fstore";
    private LogAppender<String> appender;

    private File testDirectory;
    private static final int SEGMENT_SIZE = 1024 * 1000;//1mb

    @Before
    public void setUp() throws IOException {
        testDirectory = new File(folder);
        if (!testDirectory.exists()) {
            testDirectory.mkdir();
        }
        Utils.removeFiles(testDirectory);
        appender = LogAppender.create(testDirectory, new StringSerializer(), SEGMENT_SIZE);
    }

    @After
    public void cleanup() {
        IOUtils.closeQuietly(appender);
    }

    @Test
    public void roll() {
        int written = 0;
        while (written <= SEGMENT_SIZE) {
            String data = UUID.randomUUID().toString();
            appender.append(data);
            written += data.length() + Integer.BYTES * 2; //header size
        }
        appender.append("CAUSES-ROLL");
        appender.append("new-segment");
        assertEquals(2, appender.segments.size());
    }

    @Test
    public void reader() {
        int counter = 0;

        int written = 0;
        while (written < SEGMENT_SIZE) {
            String data = String.valueOf(counter++);
            appender.append(data);
            written += data.length() + Integer.BYTES * 2; //data + header size
        }
        appender.append("LAST-ENTRY-ON-FIRST-SEGMENT");

        String lastEntry = "FIRST-ENTRY-NEXT-SEGMENT";
        appender.append(lastEntry);

        Scanner<String> scanner = appender.scanner();

        String lastValue = null;
        for (String value : scanner) {
            lastValue = value;
        }

        assertEquals(lastEntry, lastValue);
    }

    @Test
    public void position() {

        int segmentIdx = 1;
        long positionOnSegment = 32;
        long position = appender.toSegmentedPosition(segmentIdx, positionOnSegment);

        int segment = appender.getSegment(position);
        long foundPositionOnSegment = appender.getPositionOnSegment(position);

        assertEquals(segmentIdx, segment);
        assertEquals(positionOnSegment, foundPositionOnSegment);


    }

    @Test
    public void reader_position() {
        int counter = 0;

        int written = 0;
        while (written < SEGMENT_SIZE) {
            String data = String.valueOf(counter++);
            appender.append(data);
            written += data.length() + Integer.BYTES * 2; //data + header size
        }
        appender.append("LAST-ENTRY-ON-FIRST-SEGMENT");

        String lastEntry = "FIRST-ENTRY-NEXT-SEGMENT";
        long lastWrittenPosition = appender.append(lastEntry);

        Scanner<String> scanner = appender.scanner(lastWrittenPosition);

        assertTrue(scanner.hasNext());
        assertEquals(lastEntry, scanner.next());
    }

    @Test
    @Ignore
    public void large_items() {
        int counter = 0;

        long written = 0;
        while (true) {
            String data = UUID.randomUUID().toString();
            appender.append(data);
            written += data.length() + Integer.BYTES * 2; //data + header size
        }
//        System.out.println(written);
//
//        for (String item : appender.scanner()) {
//            System.out.println(item);
//        }


    }
}