package io.joshworks.fstore.es.stream;

import io.joshworks.fstore.es.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StreamsTest {

    private Streams streams;
    private File directory;

    @Before
    public void setUp() {
        directory = Utils.testFolder();
        streams = new Streams(directory, 10, streamHash -> -1);
    }

    @After
    public void tearDown() {
        streams.close();
        Utils.tryDelete(directory);
    }


    @Test
    public void get_returns_correct_stream() {
        streams.add(new StreamMetadata("a", 1, 0));
        assertTrue(streams.get(1).isPresent());
    }

    @Test
    public void get_returns_correct_stream_after_reopening() {
        streams.add(new StreamMetadata("a", 1, 0));
        streams.close();

        streams = new Streams(directory, 10, streamHash -> -1);
        assertTrue(streams.get(1).isPresent());
    }

    @Test
    public void streamsStartingWith() {

        streams.add(new StreamMetadata("abc-123", 1, 0));
        streams.add(new StreamMetadata("abc-345", 2, 0));
        streams.add(new StreamMetadata("another1", 3, 0));
        streams.add(new StreamMetadata("another2", 4, 0));

        Set<String> names = streams.streamsStartingWith("abc-");

        assertEquals(2, names.size());
        assertTrue(names.contains("abc-123"));
        assertTrue(names.contains("abc-345"));
    }

    @Test
    public void version_of_nonExisting_stream_returns_zero() {
        int version = streams.tryIncrementVersion(123, -1);
        assertEquals(0, version);
    }

    @Test(expected = IllegalArgumentException.class)
    public void unexpected_version_throws_exception() {
        streams.tryIncrementVersion(123, 1);
        fail("Expected version mismatch");
    }

    @Test
    public void existing_stream_returns_correct_version() {
        streams.tryIncrementVersion(123, -1);
        int version1 = streams.tryIncrementVersion(123, 0);
        assertEquals(1, version1);

        int version2 = streams.tryIncrementVersion(123, 1);
        assertEquals(2, version2);
    }
}