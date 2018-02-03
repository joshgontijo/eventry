package io.joshworks.fstore.log;

import io.joshworks.fstore.serializer.StringSerializer;
import io.joshworks.fstore.utils.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(RandomOrderEnforcer.class)
public class LogAppenderTest {

    private LogAppender<String> appender;
    private Path testFile;

    @Before
    public void setUp()  {
        testFile = new File("test.db").toPath();
        appender = LogAppender.create(testFile.toFile(), new StringSerializer(), 2048, 2048);
    }

    @After
    public void cleanup() throws IOException {
        IOUtils.closeQuietly(appender);
        Files.delete(testFile);
    }

    @Test(expected = IllegalArgumentException.class)
    public void minimumBuffer() {
        int minimumSize = 8;
        LogAppender.create(testFile.toFile(), new StringSerializer(), 1024, minimumSize - 1);
    }

    @Test
    public void writePosition() {
        String data = "hello";
        appender.write(data);

        assertEquals(4 + 4 + data.length(), appender.position()); // 4 + 4 (heading) + data length
    }

    @Test
    public void writePosition_reopen() {
        String data = "hello";
        appender.write(data);

        long position = appender.position();
        appender.close();
        appender = LogAppender.open(testFile.toFile(), new StringSerializer(), 2048, position);

        assertEquals(4 + 4 + data.length(), appender.position()); // 4 + 4 (heading) + data length
    }

    @Test
    public void write() {
        String data = "hello";
        appender.write(data);

        Reader<String> reader = appender.reader();
        assertTrue(reader.hasNext());
        assertEquals(data, reader.next());
        assertEquals(4 + 4 + data.length(), reader.position()); // 4 + 4 (heading) + data length
    }

    @Test
    public void checkConsistency() {
        String data = "hello";
        appender.write(data);

        assertEquals(4 + 4 + data.length(), appender.position()); // 4 + 4 (heading) + data length

        long position = appender.position();
        appender.close();
        appender = LogAppender.open(testFile.toFile(), new StringSerializer(), 2048, position, true);

        assertEquals(4 + 4 + data.length(), appender.position()); // 4 + 4 (heading) + data length

        String data2 = "aaaaaaaaaaaaaaaa";
        appender.write(data2);

        int firstEntrySize = 4 + 4 + data.length();
        Reader<String> reader = appender.reader();
        assertTrue(reader.hasNext());
        assertEquals(data, reader.next());
        assertEquals(firstEntrySize, reader.position()); // 4 + 4 (heading) + data length

        int secondEntrySize = 4 + 4 + data2.length();
        assertTrue(reader.hasNext());
        assertEquals(data2, reader.next());
        assertEquals(firstEntrySize + secondEntrySize, reader.position()); // 4 + 4 (heading) + data length
    }

    @Test(expected = CorruptedLogException.class)
    public void checkConsistency_position_ne_previous() {
        String data = "hello";
        appender.write(data);

        assertEquals(4 + 4 + data.length(), appender.position()); // 4 + 4 (heading) + data length

        long position = appender.position();
        appender.close();
        appender = LogAppender.open(testFile.toFile(), new StringSerializer(), 2048, position + 1, true);
    }

    @Test(expected = CorruptedLogException.class)
    public void checkConsistency_position_alteredData() throws IOException {
        String data = "hello";
        appender.write(data);

        assertEquals(4 + 4 + data.length(), appender.position()); // 4 + 4 (heading) + data length

        long position = appender.position();
        appender.close();

        //add some random data
        try(RandomAccessFile raf = new RandomAccessFile(testFile.toFile(), "rw")) {
            raf.writeInt(1);
        }

        appender = LogAppender.open(testFile.toFile(), new StringSerializer(), 2048, position - 1, true);
    }

    @Test
    public void reader_reopen() {
        String data = "hello";
        appender.write(data);

        Reader<String> reader = appender.reader();
        assertTrue(reader.hasNext());
        assertEquals(data, reader.next());

        long position = appender.position();
        appender.close();
        appender = LogAppender.open(testFile.toFile(), new StringSerializer(), 2048, position);

        reader = appender.reader();
        assertTrue(reader.hasNext());
        assertEquals(data, reader.next());
        assertEquals(4 + 4 + data.length(), reader.position()); // 4 + 4 (heading) + data length
    }

    @Test
    public void multiple_readers() {
        String data = "hello";
        appender.write(data);

        Reader<String> reader1 = appender.reader();
        assertTrue(reader1.hasNext());
        assertEquals(data, reader1.next());
        assertEquals(4 + 4 + data.length(), reader1.position()); // 4 + 4 (heading) + data length

        Reader<String> reader2 = appender.reader();
        assertTrue(reader2.hasNext());
        assertEquals(data, reader2.next());
        assertEquals(4 + 4 + data.length(), reader1.position()); // 4 + 4 (heading) + data length
    }

    @Test
    public void big_entry() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            sb.append(UUID.randomUUID().toString());
        }
        String data = sb.toString();
        appender.write(data);

        Reader<String> reader1 = appender.reader();
        assertTrue(reader1.hasNext());
        assertEquals(data, reader1.next());
        assertEquals(4 + 4 + data.length(), reader1.position()); // 4 + 4 (heading) + data length

    }
}