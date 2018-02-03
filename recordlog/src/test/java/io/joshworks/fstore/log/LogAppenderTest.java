package io.joshworks.fstore.log;

import io.joshworks.fstore.serializer.StringSerializer;
import io.joshworks.fstore.utils.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LogAppenderTest {

    private LogAppender<String> appender;
    private Path testFile;

    @Before
    public void setUp() throws Exception {
        testFile = new File("test.db").toPath();
        Files.deleteIfExists(testFile);
        appender = new LogAppender<>(testFile.toFile(), new StringSerializer(), 2048);
    }

    @After
    public void cleanup() throws IOException {
        IOUtils.closeQuietly(appender);
        Files.delete(testFile);
    }

    @Test(expected = IllegalArgumentException.class)
    public void minimumBuffer() {
        new LogAppender<>(testFile.toFile(), new StringSerializer(), 7);
    }

    @Test
    public void write() {
        String data = "hello";
        appender.write(data);

        Reader<String> reader = appender.reader();
        assertTrue(reader.hasNext());
        assertEquals(data, reader.next());
    }

    @Test
    public void reopen() {
        String data = "hello";
        appender.write(data);

        Reader<String> reader = appender.reader();
        assertTrue(reader.hasNext());
        assertEquals(data, reader.next());

        appender.close();
        appender = new LogAppender<>(testFile.toFile(), new StringSerializer(), 2048);

        reader = appender.reader();
        assertTrue(reader.hasNext());
        assertEquals(data, reader.next());
    }

    @Test
    public void multiple_readers() {
        String data = "hello";
        appender.write(data);

        Reader<String> reader1 = appender.reader();
        assertTrue(reader1.hasNext());
        assertEquals(data, reader1.next());

        Reader<String> reader2 = appender.reader();
        assertTrue(reader2.hasNext());
        assertEquals(data, reader2.next());


    }
}