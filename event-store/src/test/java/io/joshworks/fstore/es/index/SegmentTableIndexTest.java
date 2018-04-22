package io.joshworks.fstore.es.index;

import io.joshworks.fstore.core.io.DiskStorage;
import io.joshworks.fstore.core.io.Storage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SegmentTableIndexTest {

    private Path tempFile;
    private TableIndex index;

    @Before
    public void setUp() throws Exception {
        tempFile = Files.createTempFile("test", ".idx");
        index = new TableIndex();
    }

    @After
    public void tearDown() throws Exception {
        index.close();
        Files.delete(tempFile);
    }

    @Test
    public void range() {


    }

    @Test
    public void first() {
    }

    @Test
    public void last() {
    }

    @Test
    public void write() throws IOException {
        MemIndex memIndex = new MemIndex();
        memIndex.add(1, 0, 0);
        memIndex.add(1, 1, 0);
        memIndex.add(1, 2, 0);
        memIndex.add(1, 3, 0);

        try(Storage storage = new DiskStorage(tempFile.toFile(), 4096)) {
            SegmentIndex idx = SegmentIndex.write(memIndex, storage);

            assertEquals(2, idx.midpoints.length);

            assertEquals(memIndex.index.first(), idx.midpoints[0].key);
            assertEquals(memIndex.index.last(), idx.midpoints[idx.midpoints.length - 1].key);

        }
    }

    @Test
    public void load() throws IOException {

        MemIndex memIndex = new MemIndex();
        memIndex.add(1, 0, 0);
        memIndex.add(1, 1, 0);
        memIndex.add(1, 2, 0);
        memIndex.add(1, 3, 0);

        try(Storage storage = new DiskStorage(tempFile.toFile(), 4096)) {
            SegmentIndex idx = SegmentIndex.write(memIndex, storage);

            SegmentIndex found = SegmentIndex.load(storage);

            assertEquals(2, found.midpoints.length);
            assertEquals(idx.midpoints[0].key, found.midpoints[0].key);

            assertEquals(memIndex.index.first(), idx.midpoints[0].key);
            assertEquals(memIndex.index.last(), idx.midpoints[idx.midpoints.length - 1].key);

        }
    }

    @Test
    public void when_range_query_gt_index_bounds_return_empty_set() throws IOException {

        long stream = 1;
        long streamQuery = 2;

        MemIndex memIndex = new MemIndex();
        memIndex.add(stream, 0, 0);
        memIndex.add(stream, 1, 0);
        memIndex.add(stream, 2, 0);
        memIndex.add(stream, 3, 0);

        try(Storage storage = new DiskStorage(tempFile.toFile(), 4096)) {
            SegmentIndex idx = SegmentIndex.write(memIndex, storage);

            Range range = Range.of(streamQuery, 0);

            assertTrue(idx.range(range).isEmpty());
        }
    }

    @Test
    public void when_range_query_lt_index_bounds_return_empty_set() throws IOException {

        long stream = 2;
        long streamQuery = 1;

        MemIndex memIndex = new MemIndex();
        memIndex.add(stream, 0, 0);
        memIndex.add(stream, 1, 0);
        memIndex.add(stream, 2, 0);
        memIndex.add(stream, 3, 0);

        try(Storage storage = new DiskStorage(tempFile.toFile(), 4096)) {
            SegmentIndex idx = SegmentIndex.write(memIndex, storage);

            Range range = Range.of(streamQuery, 0);

            assertTrue(idx.range(range).isEmpty());
        }
    }

    @Test
    public void when_range_query_in_index_bounds_return_all_matches() throws IOException {

        long stream = 1;

        MemIndex memIndex = new MemIndex();
        memIndex.add(stream, 0, 0);
        memIndex.add(stream, 1, 0);
        memIndex.add(stream, 2, 0);
        memIndex.add(stream, 3, 0);

        try(Storage storage = new DiskStorage(tempFile.toFile(), 4096)) {
            SegmentIndex idx = SegmentIndex.write(memIndex, storage);

            Range range = Range.of(stream, 1, 3);

            List<IndexEntry> found = idx.range(range);
            assertEquals(2, found.size());
            assertTrue(found.contains(IndexEntry.of(stream, 1, 0)));
            assertTrue(found.contains(IndexEntry.of(stream, 2, 0)));
        }
    }

}