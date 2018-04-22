package io.joshworks.fstore.es.index;

import io.joshworks.fstore.core.io.DiskStorage;
import io.joshworks.fstore.core.io.Storage;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SegmentIndexTest {

    private Storage storage;

    @Before
    public void setUp() throws IOException {
        Path tempFile = Files.createTempFile(null, null);
        storage = new DiskStorage(tempFile.toFile());
    }

    @Test
    public void range_query_returns_the_correct_midpoint() {

        //given
        SegmentIndex diskIndex = indexWithStreamRanging(0, 1000000);

        //when
        List<IndexEntry> range = diskIndex.range(Range.allOf(0));

        //then
        assertEquals(1, range.size());
    }

    @Test
    public void loaded_segmentIndex_has_the_same_midpoints() {
        //given
        SegmentIndex diskIndex = indexWithStreamRanging(0, 1000000);

        //when
        SegmentIndex loaded = SegmentIndex.load(storage);

        //then
        assertTrue(Arrays.equals(diskIndex.midpoints, loaded.midpoints));
    }

    @Test
    public void range_query_returns_the_correct_midpoint_for_negative_hash() {
        //given
        SegmentIndex diskIndex = indexWithStreamRanging(-5, 0);

        //when
        List<IndexEntry> range = diskIndex.range(Range.allOf(-5));

        //then
        assertEquals(1, range.size());
    }

    @Test
    public void range_query_returns_all_version_of_stream() {

        //given
        long stream = 0;
        int startVersion = 1;
        int endVersion = 10;
        SegmentIndex diskIndex = indexWithSameStreamWithVersionRanging(stream, startVersion, endVersion);

        //when
        List<IndexEntry> found = diskIndex.range(Range.allOf(stream));

        //then
        assertEquals(10, found.size());
    }

    @Test
    public void entries_that_fit_in_one_page_should_be_loaded_all_at_once() {

        //given
        int numEntries = 10;
        SegmentIndex diskIndex = indexWithStreamRanging(1, numEntries);

        //when
        Collection<IndexEntry> entries = diskIndex.loadPage(SegmentIndex.HEADER_SIZE); //start after header

        //then
        assertEquals(numEntries, entries.size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void loading_page_starting_at_header_throws_error() {

        //given
        int numEntries = 10;
        SegmentIndex diskIndex = indexWithStreamRanging(0, numEntries);

        //when
        diskIndex.loadPage(SegmentIndex.HEADER_SIZE - 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void loading_page_starting_after_max_position_throws_error() {

        //given
        int numEntries = 10;
        SegmentIndex diskIndex = indexWithStreamRanging(0, numEntries);

        //when
        long pos = (diskIndex.entries() * IndexEntry.BYTES) + SegmentIndex.HEADER_SIZE;
        diskIndex.loadPage(pos);

    }

    @Test(expected = IllegalArgumentException.class)
    public void loading_unaligned_page_throws_error() {

        //given
        int numEntries = 10;
        SegmentIndex diskIndex = indexWithStreamRanging(0, numEntries);

        //when
        long pos = SegmentIndex.HEADER_SIZE + 1;
        diskIndex.loadPage(pos);
    }

    @Test
    public void first_returns_the_first_key_in_the_index() {
        //given
        SegmentIndex diskIndex = indexWithStreamRanging(0, 10);

        //when
        IndexEntry first = diskIndex.first();

        //then
        assertEquals(0, first.stream);
    }

    @Test
    public void last_returns_the_last_key_in_the_index() {
        //given
        SegmentIndex diskIndex = indexWithStreamRanging(1, 10);

        //when
        IndexEntry last = diskIndex.last();

        //then
        assertEquals(10, last.stream);
    }

    @Test
    public void write_save_file_to_disk() {

        //given
        MemIndex index = new MemIndex();
        index.add(0,0,0);

        //when
        SegmentIndex write = SegmentIndex.write(index, storage);

        //then
        assertTrue(storage.size() > 0);
    }

    @Test
    public void latestStreamVersion_with_index_full_of_the_same_stream() {

        //given
        long stream = 123L;
        int lastVersion = 500;
        SegmentIndex diskIndex = indexWithSameStreamWithVersionRanging(stream, 1, lastVersion);

        //when
        Optional<IndexEntry> indexEntry = diskIndex.latestOfStream(stream);

        //then
        assertTrue(indexEntry.isPresent());
        assertEquals(lastVersion, indexEntry.get().version);
    }

    @Test
    public void latestStreamVersion_with_index_multiple_streams() {

        //given
        long stream = 123L;
        SegmentIndex diskIndex = indexWithStreamRanging(1, 500);

        //when
        for (int i = 1; i <= 500; i++) {
            Optional<IndexEntry> indexEntry = diskIndex.latestOfStream(stream);

            //then
            assertTrue(indexEntry.isPresent());
            assertEquals(1, indexEntry.get().version);
        }

    }

    private SegmentIndex indexWithStreamRanging(int from, int to) {
        //given
        MemIndex memIndex = new MemIndex();
        for (int i = from; i <= to; i++) {
            memIndex.add(i, 1, 0);
        }
        return SegmentIndex.write(memIndex, storage);
    }

    private SegmentIndex indexWithSameStreamWithVersionRanging(long stream, int from, int to) {
        //given
        MemIndex memIndex = new MemIndex();
        for (int i = from; i <= to; i++) {
            memIndex.add(stream, i, 0);
        }
        return SegmentIndex.write(memIndex, storage);
    }

}