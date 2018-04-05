package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.io.DiskStorage;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.Log;
import io.joshworks.fstore.log.Scanner;
import io.joshworks.fstore.log.Utils;
import io.joshworks.fstore.serializer.StandardSerializer;
import io.joshworks.fstore.serializer.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class LogAppenderTest {

    private LogAppender<String> appender;

    private static final int SEGMENT_SIZE = 1024 * 10;//10kb

    protected abstract LogAppender<String> appender(Builder<String> builder);

    File testDirectory;

    @Before
    public void setUp() throws IOException {
        String dir = ".fstoreTest";
        testDirectory = new File(dir);
        if (testDirectory.exists()) {
            Utils.tryDelete(testDirectory);
        }
        testDirectory = Files.createTempDirectory(dir).toFile();
        testDirectory.deleteOnExit();

        Builder<String> builder = new Builder<>(testDirectory, new StringSerializer())
                .segmentSize(SEGMENT_SIZE);

        appender = appender(builder);
    }

    @After
    public void cleanup() {
        IOUtils.closeQuietly(appender);
        Utils.tryDelete(testDirectory);
    }

    @Test
    public void roll() {
        StringBuilder sb = new StringBuilder();
        while (sb.length() <= SEGMENT_SIZE) {
            sb.append(UUID.randomUUID().toString());
        }
        appender.append(sb.toString());
        appender.append("new-segment");
        assertEquals(2, appender.segments.size());
    }

    @Test
    public void roll_timeBased() throws IOException {
        File testDirectory = Files.createTempDirectory(".fstoreTest2").toFile();
        try {
            testDirectory.deleteOnExit();
            if (testDirectory.exists()) {
                Utils.tryDelete(testDirectory);
            }

            try (LogAppender<String> testAppender = appender(new Builder<>(testDirectory, StandardSerializer.of(String.class)).rollInterval(Duration.ofSeconds(2)))) {
                testAppender.append("1");

                Utils.sleep(2100);

                testAppender.append("2");
                testAppender.append("3");
            }

            try (LogAppender<String> testAppender = appender(new Builder<>(testDirectory, StandardSerializer.of(String.class)))) {

                assertEquals(2, testAppender.segments.size());

                Set<String> values = testAppender.stream().collect(Collectors.toSet());
                assertTrue(values.contains("1"));
                assertTrue(values.contains("2"));
                assertTrue(values.contains("3"));
            }
        } finally {
            Utils.tryDelete(testDirectory);
        }
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
        long pos2 = appender.append("2");
        long pos3 = appender.append("3");

        appender.flush();
        Scanner<String> scanner = appender.scanner();

        assertEquals(pos1, scanner.position());
        String found = scanner.next();
        assertEquals("1", found);

        assertEquals(pos2, scanner.position());
        found = scanner.next();
        assertEquals("2", found);

        assertEquals(pos3, scanner.position());
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
    public void reopen() throws IOException {
        File testDirectory = Files.createTempDirectory(".fstoreTest2").toFile();
        try {
            testDirectory.deleteOnExit();
            if (testDirectory.exists()) {
                Utils.tryDelete(testDirectory);
            }

            try (LogAppender<String> testAppender = appender(new Builder<>(testDirectory, StandardSerializer.of(String.class)))) {
                testAppender.append("1");
                testAppender.append("2");
                testAppender.append("3");
            }
            try (LogAppender<String> testAppender = appender(new Builder<>(testDirectory, StandardSerializer.of(String.class)))) {
                testAppender.append("4");
            }
            try (LogAppender<String> testAppender = appender(new Builder<>(testDirectory, StandardSerializer.of(String.class)))) {
                Set<String> values = testAppender.stream().collect(Collectors.toSet());
                assertTrue(values.contains("1"));
                assertTrue(values.contains("2"));
                assertTrue(values.contains("3"));
                assertTrue(values.contains("4"));
            }
        } finally {
            Utils.tryDelete(testDirectory);
        }
    }

    @Test
    public void name() {

//        System.out.println(Long.toBinaryString(22L));
//        System.out.println("------------");
//        System.out.println(Long.toBinaryString(4194297L));
//        System.out.println(Long.toBinaryString(4194297L >> 22L));
//        System.out.println("------------");
//        System.out.println(Long.toBinaryString(4194306L));
//        System.out.println(Long.toBinaryString(4194306L >> 22L));

//        printLong(22L);
//        System.out.println("------------");
//        printLong(4194297L);
//        printLong(4194297L >> 22L);
//        System.out.println("------------");
//        printLong(4194306L);
//        printLong(4194306L >> 22L);


//        System.out.println(Long.toBinaryString(3L << 62));
//        System.out.println(Long.toBinaryString(-1L));
//        System.out.println(Long.toBinaryString(1L));
//        System.out.println(Long.toBinaryString(3L << Long.numberOfLeadingZeros(3L)));
//        System.out.println(3L << Long.numberOfLeadingZeros(3L));
        System.out.println(Long.toBinaryString(22L));
        System.out.println("------------");
        System.out.println(Long.toBinaryString(4194297L));
        System.out.println(Long.toBinaryString(4194297L >> 22L));
        System.out.println("------------");
        System.out.println(Long.toBinaryString(4194306L));
        System.out.println(Long.toBinaryString(4194306L >> 22L));
    }

    private void printLong(long val) {
        for (int i = 0; i < Long.numberOfLeadingZeros(val); i++) {
            System.out.print('0');
        }
        System.out.println(Long.toBinaryString(val));
    }

    @Test
    public void reopen_brokenEntry() throws IOException {
        File testDirectory = Files.createTempDirectory(".fstoreTest2").toFile();
        try {
            testDirectory.deleteOnExit();
            if (testDirectory.exists()) {
                Utils.tryDelete(testDirectory);
            }

            String segmentName;
            long lastPosition;
            try (LogAppender<String> testAppender = appender(new Builder<>(testDirectory, StandardSerializer.of(String.class)))) {
                testAppender.append("1");
                testAppender.append("2");
                testAppender.append("3");

                //get last segment (in this case there will be always one)
                segmentName = testAppender.segments().get(testAppender.segments().size() - 1);
                lastPosition = testAppender.currentSegment.position();
            }

            //write broken data

            try (Storage storage = new DiskStorage(new File(testDirectory, segmentName))) {
                ByteBuffer broken = ByteBuffer.allocate(Log.HEADER_SIZE + 4);
                broken.putInt(444); //expected length
                broken.putInt(123); // broken checksum
                broken.putChar('A'); // broken data
                storage.write(lastPosition, broken);
            }

            try (LogAppender<String> testAppender = appender(new Builder<>(testDirectory, StandardSerializer.of(String.class)))) {
                testAppender.append("4");
            }

            try (LogAppender<String> testAppender = appender(new Builder<>(testDirectory, StandardSerializer.of(String.class)))) {
                Set<String> values = testAppender.stream().collect(Collectors.toSet());
                assertTrue(values.contains("1"));
                assertTrue(values.contains("2"));
                assertTrue(values.contains("3"));
                assertTrue(values.contains("4"));
            }


        } finally {
            Utils.tryDelete(testDirectory);
        }
    }

    @Test
    public void segmentBitShift() {
        for (int i = 0; i < appender.maxSegments; i++) {
            appender.segments.add(null);
            long position = appender.toSegmentedPosition(i, 0);
            long foundSegment = appender.getSegment(position);
            assertEquals("Failed on segIdx " + i + " - position: " + position + " - foundSegment: " + foundSegment, i, foundSegment);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void toSegmentedPosition_invalid() {
        appender.segments.add(null);
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
}