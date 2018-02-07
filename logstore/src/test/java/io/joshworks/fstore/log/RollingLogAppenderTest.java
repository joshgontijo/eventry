package io.joshworks.fstore.log;

import io.joshworks.fstore.serializer.StringSerializer;
import io.joshworks.fstore.utils.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RollingLogAppenderTest {

    private static final String folder = System.getProperty("user.home") + File.separator + ".fstore";
    private Log<String> appender;

    private File testDirectory;
    private static final int SEGMENT_SIZE = 1024 * 1000;//1mb

    @Before
    public void setUp() throws IOException {
        testDirectory = new File(folder);
        if (!testDirectory.exists()) {
            testDirectory.mkdir();
        }
        Utils.removeFiles(testDirectory);
        appender = RollingLogAppender.create(testDirectory, new StringSerializer(), SEGMENT_SIZE);
    }

    @After
    public void cleanup() {
        IOUtils.closeQuietly(appender);
    }

    @Test
    public void position() {
        long position1 = appender.append("test1");
        assertTrue(position1 > 0);

        long position2 = appender.append("test2");
        assertTrue(position2 > position1);
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
        long newSegmentPosition = appender.append("new-segment");
        assertEquals(RollingLogAppender.SEGMENT_MULTIPLIER * 2, newSegmentPosition);
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
        long lastWrittenPosition = appender.append(lastEntry);

        Scanner<String> scanner = appender.scanner();

        String lastValue = null;
        long lastPosition = -1;
        for (String value : scanner) {
            lastValue = value;
            lastPosition = scanner.position();
        }

        assertEquals(lastEntry, lastValue);
        assertEquals(lastWrittenPosition + lastEntry.length() + Integer.BYTES * 2, lastPosition);
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
        assertEquals(lastWrittenPosition + lastEntry.length() + Integer.BYTES * 2, scanner.position());
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