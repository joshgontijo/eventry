package io.joshworks.fstore.es.index;

import io.joshworks.fstore.es.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.Assert.*;

public class TableIndexTest {

    private TableIndex tableIndex;
    private File testDirectory;

    @Before
    public void setUp() throws Exception {
        tableIndex = new TableIndex();
        testDirectory = Files.createTempDirectory(null).toFile();
    }

    @After
    public void tearDown() throws Exception {
        tableIndex.close();
        Utils.tryDelete(testDirectory);
    }

    @Test
    public void event_version_is_on_ascending_order() {

        long stream = 1;

        tableIndex.add(stream, 1, 0);
        tableIndex.flush(testDirectory, "yolo1");
        tableIndex.add(stream, 2, 0);
        tableIndex.flush(testDirectory, "yolo2");
        tableIndex.add(stream, 3, 0);
        tableIndex.flush(testDirectory, "yolo3");
        tableIndex.add(stream, 4, 0); //memory

        Iterator<IndexEntry> it = tableIndex.iterator();

        assertEquals(1, it.next().version);
        assertEquals(2, it.next().version);
        assertEquals(3, it.next().version);
        assertEquals(4, it.next().version);
    }

    @Test
    public void data_added_to_the_inmemory_can_be_retrieved() {

        long stream = 1;
        int version = 1;
        tableIndex.add(stream, version, 0);
        Optional<IndexEntry> indexEntry = tableIndex.get(stream, version);

        assertTrue(indexEntry.isPresent());
        assertEquals(stream, indexEntry.get().stream);
        assertEquals(version, indexEntry.get().version);
    }

    @Test
    public void data_added_to_the_disk_is_retrieved() {

        long stream = 1;
        int version = 1;
        tableIndex.add(stream, version, 0);

        tableIndex.flush(testDirectory, "yolo");

        Optional<IndexEntry> indexEntry = tableIndex.get(stream, version);

        assertTrue(indexEntry.isPresent());
        assertEquals(stream, indexEntry.get().stream);
        assertEquals(version, indexEntry.get().version);
    }

    @Test
    public void version_added_to_memory_is_retrieved() {
        long stream = 1;
        int version = 1;
        tableIndex.add(stream, version, 0);

        int foundVersion = tableIndex.version(stream);

        assertEquals(version, foundVersion);
    }

    @Test
    public void version_added_to_isk_is_retrieved() {
        long stream = 1;
        int version = 1;
        tableIndex.add(stream, version, 0);

        tableIndex.flush(testDirectory, "yolo");

        int foundVersion = tableIndex.version(stream);

        assertEquals(version, foundVersion);
    }

    @Test
    public void size_returns_the_total_of_inmemory_and_disk() {
        long stream = 1;
        tableIndex.add(stream, 1, 0);

        tableIndex.flush(testDirectory, "yolo");

        tableIndex.add(stream, 2, 0);

        long size = tableIndex.size();

        assertEquals(2, size);
    }

    @Test
    public void stream_returns_data_from_inmemory_and_disk() {
        long stream = 1;
        tableIndex.add(stream, 1, 0);

        tableIndex.flush(testDirectory, "yolo");

        tableIndex.add(stream, 2, 0);

        Stream<IndexEntry> dataStream = tableIndex.stream();

        assertEquals(2, dataStream.count());

        Iterator<IndexEntry> it = tableIndex.stream().iterator();

        assertEquals(1, it.next().version);
        assertEquals(2, it.next().version);
    }

    @Test
    public void range_stream_with_range_returns_data_from_disk_and_memory() {
        long stream = 1;

        tableIndex.add(stream, 1, 0);
        tableIndex.flush(testDirectory, "yolo");
        tableIndex.add(stream, 2, 0);

        Stream<IndexEntry> dataStream = tableIndex.stream(Range.allOf(stream));

        assertEquals(2, dataStream.count());

        Iterator<IndexEntry> it = tableIndex.stream().iterator();

        assertEquals(1, it.next().version);
        assertEquals(2, it.next().version);

    }


    @Test
    public void get() {
    }

    @Test
    public void iterator1() {
    }
}