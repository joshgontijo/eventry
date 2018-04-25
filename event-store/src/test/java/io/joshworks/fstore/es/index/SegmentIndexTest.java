package io.joshworks.fstore.es.index;

import io.joshworks.fstore.core.io.DiskStorage;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.es.index.filter.BloomFilter;
import io.joshworks.fstore.index.filter.Hash;
import io.joshworks.fstore.serializer.Serializers;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
    public void loaded_segmentIndex_has_the_same_10_midpoints() {
        //given
        SegmentIndex diskIndex = indexWithStreamRanging(1, 10);

        //when
        SegmentIndex loaded = SegmentIndex.load(storage);

        //then
        assertTrue(Arrays.equals(diskIndex.midpoints, loaded.midpoints));
    }

    @Test
    public void loaded_segmentIndex_has_the_same_1000000_midpoints() {
        //given
        SegmentIndex diskIndex = indexWithStreamRanging(1, 1000000);

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
        Collection<IndexEntry> entries = diskIndex.readPage(SegmentIndex.HEADER_SIZE); //start after header

        //then
        assertEquals(numEntries, entries.size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void loading_page_starting_at_header_throws_error() {

        //given
        int numEntries = 10;
        SegmentIndex diskIndex = indexWithStreamRanging(0, numEntries);

        //when
        diskIndex.readPage(SegmentIndex.HEADER_SIZE - 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void loading_page_starting_after_max_position_throws_error() {

        //given
        int numEntries = 10;
        SegmentIndex diskIndex = indexWithStreamRanging(0, numEntries);

        //when
        long pos = (diskIndex.entries() * IndexEntry.BYTES) + SegmentIndex.HEADER_SIZE;
        diskIndex.readPage(pos);

    }

    @Test(expected = IllegalArgumentException.class)
    public void loading_unaligned_page_throws_error() {

        //given
        int numEntries = 10;
        SegmentIndex diskIndex = indexWithStreamRanging(1, numEntries);

        //when
        long pos = SegmentIndex.HEADER_SIZE + 1;
        diskIndex.readPage(pos);
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
    public void range() {
        //given
        int startStream = 1;
        int endStream = 100000;
        SegmentIndex diskIndex = indexWithStreamRanging(startStream, endStream);

        //when
        for (int i = startStream; i < endStream; i++) {
            List<IndexEntry> last = diskIndex.range(Range.allOf(i));

            //then
            assertEquals("Failed on position " + i, 1, last.size());
        }
    }

    @Test
    public void latestStreamVersion_with_index_multiple_streams() {

        //given
        int startStream = 1;
        int endStream = 100000;
        SegmentIndex diskIndex = indexWithStreamRanging(startStream, endStream);

        //when
        for (int i = startStream; i <= endStream; i++) {
            Optional<IndexEntry> indexEntry = diskIndex.latestOfStream(i);

            //then
            assertTrue("Failed on iteration " + i, indexEntry.isPresent());
            assertEquals("Failed on iteration " + i, 1, indexEntry.get().version);
        }
    }

    @Test
    public void bloom_filter_usage() {

        //given
        MemIndex memIndex = new MemIndex();
        memIndex.add(1, 0, 0);
        memIndex.add(2, 0, 0);
        memIndex.add(3, 0, 0);
        memIndex.add(5, 0, 0);

        //when
        int someOtherStream = 4;
        SegmentIndex diskIndex = SegmentIndex.write(memIndex, storage);
        List<IndexEntry> range = diskIndex.range(Range.allOf(someOtherStream));
        assertEquals(0, range.size());
    }

    @Test
    public void bloom_filter() {
        //given
        BloomFilter<Long> filter = new BloomFilter<>(5, 0.5, new Hash.Murmur64<>(Serializers.LONG));
        filter.add(1L);
        filter.add(2L);
        filter.add(3L);
        filter.add(5L);

        assertTrue(filter.contains(1L));
        assertTrue(filter.contains(2L));
        assertTrue(filter.contains(3L));
        assertTrue(filter.contains(5L));
        assertFalse(filter.contains(4L));
    }

    @Test
    public void iterator_return_all_entries_with_index_containing_same_stream() {

        long stream = 1;
        int latestVersion = 1000;
        SegmentIndex diskIndex = indexWithSameStreamWithVersionRanging(stream, 1, latestVersion);

        for (int i = 0; i < latestVersion; i++) {
            //when
            Iterator<IndexEntry> iterator = diskIndex.iterator(Range.allOf(stream));

            //then
            assertIteratorHasAllEntries(stream, latestVersion, iterator);
        }
    }

    @Test
    public void iterator_return_all_entries_with_index_containing_multiple_streams() {

        int numStreams = 1000;
        SegmentIndex diskIndex = indexWithStreamRanging(1, numStreams);

        for (int stream = 1; stream < numStreams; stream++) {
            //when
            Iterator<IndexEntry> iterator = diskIndex.iterator(Range.allOf(stream));

            //then
            assertTrue(iterator.hasNext());
            IndexEntry next = iterator.next();

            assertEquals(stream, next.stream);
            assertEquals(1, next.version);
            assertFalse("Failed on " + stream, iterator.hasNext());
        }
    }

    @Test
    public void iterator_with_multiple_events_and_streams() {

        int numStreams = 200;
        int numEvents = 500;

        SegmentIndex diskIndex = indexWithXStreamsWithYEventsEach(numStreams, numEvents);

        for (int stream = 0; stream < numStreams; stream++) {
            Iterator<IndexEntry> iterator = diskIndex.iterator(Range.allOf(stream));
            assertIteratorHasAllEntries(stream, numEvents, iterator);
        }
    }

    @Test
    public void full_scan_iterator() {

        int numStreams = 200;
        int numEvents = 500;

        SegmentIndex diskIndex = indexWithXStreamsWithYEventsEach(numStreams, numEvents);

        Iterator<IndexEntry> iterator = diskIndex.iterator();

        int count = 0;
        IndexEntry preciousEntry = null;
        while(iterator.hasNext()) {
            IndexEntry current = iterator.next();
            if(preciousEntry != null) {
                assertTrue(current.greaterThan(preciousEntry));
            }
            preciousEntry = current;
            count++;
        }

        assertEquals(numStreams * numEvents, count);
    }

    private void assertIteratorHasAllEntries(long stream, int latestVersion, Iterator<IndexEntry> iterator) {
        int previousVersion = 0;
        int count = 0;
        while(iterator.hasNext()) {
            IndexEntry next = iterator.next();

            assertEquals(stream, next.stream);
            assertEquals(previousVersion + 1, next.version);
            previousVersion = next.version;
            count++;
        }

        assertEquals(latestVersion, previousVersion);
        assertEquals(latestVersion, count);
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

    private SegmentIndex indexWithXStreamsWithYEventsEach(int streamQtd, int eventsPerStream) {
        //given
        MemIndex memIndex = new MemIndex();
        for (int stream = 0; stream < streamQtd; stream++) {
            for (int version = 1; version <= eventsPerStream; version++) {
                memIndex.add(stream, version, 0);
            }
        }
        return SegmentIndex.write(memIndex, storage);
    }

}