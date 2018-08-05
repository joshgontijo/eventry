package io.joshworks.fstore.es.index;

import io.joshworks.fstore.es.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TableIndexTest {

    private TableIndex tableIndex;
    private File testDirectory;
    private static final int FLUSH_THRESHOLD = 1000000;

    @Before
    public void setUp() {
        testDirectory = Utils.testFolder();
        tableIndex = new TableIndex(testDirectory, FLUSH_THRESHOLD);
    }

    @After
    public void tearDown() {
        tableIndex.close();
        Utils.tryDelete(new File(testDirectory, "index"));
        Utils.tryDelete(testDirectory);
    }

    @Test
    public void event_version_is_in_ascending_order() {

        long stream = 1;

        tableIndex.add(stream, 0, 0);
        tableIndex.flush();
        tableIndex.add(stream, 1, 0);
        tableIndex.flush();
        tableIndex.add(stream, 2, 0);
        tableIndex.flush();
        tableIndex.add(stream, 3, 0); //memory

        Iterator<IndexEntry> it = tableIndex.iterator();

        tableIndex.forEach(System.out::println);

        assertEquals(0, it.next().version);
        assertEquals(1, it.next().version);
        assertEquals(2, it.next().version);
        assertEquals(3, it.next().version);
    }

    @Test
    public void data_added_to_the_inMemory_can_be_retrieved() {

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
        int version = 0;
        tableIndex.add(stream, version, 0);

        tableIndex.flush();

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
    public void version_added_to_disk_is_retrieved() {
        long stream = 1;
        int version = 0;
        tableIndex.add(stream, version, 0);

        tableIndex.flush();

        int foundVersion = tableIndex.version(stream);

        assertEquals(version, foundVersion);
    }

    @Test
    public void size_returns_the_total_of_inMemory_and_disk() {
        long stream = 1;
        tableIndex.add(stream, 1, 0);

        tableIndex.flush();

        tableIndex.add(stream, 2, 0);

        long size = tableIndex.size();

        assertEquals(2, size);
    }

    @Test
    public void stream_returns_data_from_inMemory_and_disk() {
        long stream = 1;
        tableIndex.add(stream, 1, 0);

        tableIndex.flush();

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

        tableIndex.add(stream, 0, 0);
        tableIndex.flush();
        tableIndex.add(stream, 1, 0);

        Stream<IndexEntry> dataStream = tableIndex.stream(Range.allOf(stream));

        assertEquals(2, dataStream.count());

        Iterator<IndexEntry> it = tableIndex.stream().iterator();

        assertEquals(0, it.next().version);
        assertEquals(1, it.next().version);

    }

    @Test
    public void stream_returns_data_from_disk_and_memory() {

        //given
        long stream = 1;
        //2 segments + in memory
        int size = (FLUSH_THRESHOLD * 2) + FLUSH_THRESHOLD / 2;
        for (int i = 0; i < size; i++) {
            tableIndex.add(stream, i, 0);
        }

        Stream<IndexEntry> dataStream = tableIndex.stream();

        assertEquals(size, dataStream.count());

        Iterator<IndexEntry> it = tableIndex.stream().iterator();

        assertEquals(0, it.next().version);
        assertEquals(1, it.next().version);

    }

    @Test
    public void iterator_runs_forward_in_the_log() {

        //given
        long stream = 1;
        //2 segments + in memory
        int size = (FLUSH_THRESHOLD * 2) + FLUSH_THRESHOLD / 2;
        for (int i = 0; i < size; i++) {
            tableIndex.add(stream, i, 0);
        }

        Iterator<IndexEntry> it = tableIndex.iterator();

        int expectedVersion = 0;
        while (it.hasNext()) {
            IndexEntry next = it.next();
            assertEquals(expectedVersion, next.version);
            expectedVersion = next.version + 1;
        }
    }

    @Test
    public void stream_with_range_returns_data_from_disk_and_memory() {

        //given
        long stream = 1;
        //2 segments + in memory
        int size = (FLUSH_THRESHOLD * 2) + FLUSH_THRESHOLD / 2;
        for (int i = 0; i < size; i++) {
            tableIndex.add(stream, i, 0);
        }

        Stream<IndexEntry> dataStream = tableIndex.stream(Range.allOf(stream));
        assertEquals(size, dataStream.count());
    }

    @Test
    public void reopened_index_returns_all_items() {

        //given
        long stream = 1;
        //1 segment + in memory
        int size = FLUSH_THRESHOLD + (FLUSH_THRESHOLD / 2);
        for (int i = 0; i < size; i++) {
            tableIndex.add(stream, i, 0);
        }

        tableIndex.close();

        tableIndex = new TableIndex(testDirectory, FLUSH_THRESHOLD);

        Stream<IndexEntry> dataStream = tableIndex.stream();

        assertEquals(size, dataStream.count());

        Iterator<IndexEntry> it = tableIndex.stream().iterator();

        assertEquals(0, it.next().version);
        assertEquals(1, it.next().version);

    }

    @Test
    public void reopened_index_returns_all_items_for_stream_rang() {

        //given
        long stream = 1;
        //1 segment + in memory
        int size = FLUSH_THRESHOLD + (FLUSH_THRESHOLD / 2);
        for (int i = 0; i <= size; i++) {
            tableIndex.add(stream, i, 0);
        }

        tableIndex.close();

        tableIndex = new TableIndex(testDirectory, FLUSH_THRESHOLD);

        Stream<IndexEntry> dataStream = tableIndex.stream(Range.of(stream, 1, 11));

        assertEquals(10, dataStream.count());

        Iterator<IndexEntry> it = tableIndex.stream(Range.of(stream, 1, 11)).iterator();

        assertEquals(1, it.next().version);
        assertEquals(2, it.next().version);
        assertEquals(3, it.next().version);
        assertEquals(4, it.next().version);
        assertEquals(5, it.next().version);
        assertEquals(6, it.next().version);
        assertEquals(7, it.next().version);
        assertEquals(8, it.next().version);
        assertEquals(9, it.next().version);
        assertEquals(10, it.next().version);

    }

    @Test
    public void version_is_returned() {

        tableIndex.close();

        //given
        int streams = 100000;
        try (TableIndex index = new TableIndex(testDirectory, 500000)) {

            for (int i = 0; i < streams; i++) {
                index.add(i, 1, 0);
            }

            for (int i = 0; i < streams; i++) {
                //when
                int version = index.version(i);
                //then
                assertEquals(1, version);
            }
        }


    }
}