package io.joshworks.fstore.es.index;

import io.joshworks.fstore.es.index.filter.BloomFilter;
import io.joshworks.fstore.index.filter.Hash;
import io.joshworks.fstore.serializer.Serializers;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IndexSegmentTest {

    private File indexFile;

    @Before
    public void setUp() throws IOException {
        indexFile = Files.createTempFile(null, null).toFile();
    }

    @Test
    public void range_query_returns_the_correct_midpoint() {

        //given
        IndexSegment diskIndex = indexWithStreamRanging(0, 1000000);

        //when
        long size = diskIndex.stream(Range.allOf(0)).count();

        //then
        assertEquals(1L, size);
    }

    @Test
    public void loaded_segmentIndex_has_the_same_10_midpoints() {
        //given
        IndexSegment diskIndex = indexWithStreamRanging(1, 10);

        //when
        IndexSegment loaded = IndexSegment.load(indexFile);

        //then
        assertTrue(Arrays.equals(diskIndex.midpoints, loaded.midpoints));
    }

    @Test
    public void loaded_segmentIndex_has_the_same_1000000_midpoints() {
        //given
        IndexSegment diskIndex = indexWithStreamRanging(1, 1000000);

        //when
        IndexSegment loaded = IndexSegment.load(indexFile);

        //then
        assertTrue(Arrays.equals(diskIndex.midpoints, loaded.midpoints));
    }

    @Test
    public void range_query_returns_the_correct_midpoint_for_negative_hash() {
        //given
        IndexSegment diskIndex = indexWithStreamRanging(-5, 0);

        //when
        long size = diskIndex.stream(Range.allOf(-5)).count();

        //then
        assertEquals(1, size);
    }

    @Test
    public void range_query_returns_all_version_of_stream() {

        //given
        long stream = 0;
        int startVersion = 1;
        int endVersion = 10;
        IndexSegment diskIndex = indexWithSameStreamWithVersionRanging(stream, startVersion, endVersion);

        //when
        long size = diskIndex.stream(Range.allOf(stream)).count();

        //then
        assertEquals(10, size);
    }

    @Test
    public void entries_that_fit_in_one_page_should_be_loaded_all_at_once() {

        //given
        int numEntries = 10;
        IndexSegment diskIndex = indexWithStreamRanging(1, numEntries);

        //when
        Collection<IndexEntry> entries = diskIndex.readPage(IndexSegment.HEADER_SIZE); //start after header

        //then
        assertEquals(numEntries, entries.size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void loading_page_starting_at_header_throws_error() {

        //given
        int numEntries = 10;
        IndexSegment diskIndex = indexWithStreamRanging(0, numEntries);

        //when
        diskIndex.readPage(IndexSegment.HEADER_SIZE - 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void loading_page_starting_after_max_position_throws_error() {

        //given
        int numEntries = 10;
        IndexSegment diskIndex = indexWithStreamRanging(0, numEntries);

        //when
        long pos = (diskIndex.size() * IndexEntry.BYTES) + IndexSegment.HEADER_SIZE;
        diskIndex.readPage(pos);

    }

    @Test
    public void midpoint_return_the_previous_index_for_non_exact_match() {

        //given
        Midpoint m1 = new Midpoint(IndexEntry.of(1, 0, 0), 0);
        Midpoint m2 = new Midpoint(IndexEntry.of(10, 0, 0), 0);
        Midpoint m3 = new Midpoint(IndexEntry.of(100, 0, 0), 0);

        IndexEntry key = IndexEntry.of(2, 0, 0);

        //when
        int idx = IndexSegment.getMidpointIdx(new Midpoint[]{m1, m2, m3}, key);

        //then
        assertEquals(0, idx);
    }

    @Test
    public void midpoint_return_the_previous_index_for_exact_match() {

        //given
        Midpoint m1 = new Midpoint(IndexEntry.of(1, 0, 0), 0);
        Midpoint m2 = new Midpoint(IndexEntry.of(10, 0, 0), 0);
        Midpoint m3 = new Midpoint(IndexEntry.of(100, 0, 0), 0);

        IndexEntry key = IndexEntry.of(1, 0, 0);

        //when
        int idx = IndexSegment.getMidpointIdx(new Midpoint[]{m1, m2, m3}, key);

        //then
        assertEquals(0, idx);
    }

    @Test(expected = IllegalArgumentException.class)
    public void loading_unaligned_page_throws_error() {

        //given
        int numEntries = 10;
        IndexSegment diskIndex = indexWithStreamRanging(1, numEntries);

        //when
        long pos = IndexSegment.HEADER_SIZE + 1;
        diskIndex.readPage(pos);
    }

    @Test
    public void first_returns_the_first_key_in_the_index() {
        //given
        IndexSegment diskIndex = indexWithStreamRanging(0, 10);

        //when
        IndexEntry first = diskIndex.first();

        //then
        assertEquals(0, first.stream);
    }

    @Test
    public void last_returns_the_last_key_in_the_index() {
        //given
        IndexSegment diskIndex = indexWithStreamRanging(1, 10);

        //when
        IndexEntry last = diskIndex.last();

        //then
        assertEquals(10, last.stream);
    }

    @Test
    public void write_save_file_to_disk() {

        //given
        MemIndex index = new MemIndex();
        index.add(0, 0, 0);

        //when
        IndexSegment write = IndexSegment.write(index, indexFile);

        //then
        assertTrue(indexFile.length() > 0);
    }

    @Test
    public void latestStreamVersion_with_index_full_of_the_same_stream() {

        //given
        long stream = 123L;
        int lastVersion = 500;
        IndexSegment diskIndex = indexWithSameStreamWithVersionRanging(stream, 1, lastVersion);

        //when
        int version = diskIndex.version(stream);

        //then
        assertEquals(lastVersion, version);
    }

    @Test
    public void range() {
        //given
        int startStream = 1;
        int endStream = 100000;
        IndexSegment diskIndex = indexWithStreamRanging(startStream, endStream);

        //when
        for (int i = startStream; i < endStream; i++) {
            long size = diskIndex.stream(Range.allOf(i)).count();

            //then
            assertEquals("Failed on position " + i, 1, size);
        }
    }

    @Test
    public void latestStreamVersion_with_index_multiple_streams() {

        //given
        int startStream = 1;
        int endStream = 100000;
        IndexSegment diskIndex = indexWithStreamRanging(startStream, endStream);

        //when
        for (int i = startStream; i <= endStream; i++) {
            int version = diskIndex.version(i);

            //then
            assertEquals("Failed on iteration " + i, 1, version);
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
        IndexSegment diskIndex = IndexSegment.write(memIndex, indexFile);
        long size = diskIndex.stream(Range.allOf(someOtherStream)).count();
        assertEquals(0, size);
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
        IndexSegment diskIndex = indexWithSameStreamWithVersionRanging(stream, 1, latestVersion);

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
        IndexSegment diskIndex = indexWithStreamRanging(1, numStreams);

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

        IndexSegment diskIndex = indexWithXStreamsWithYEventsEach(numStreams, numEvents);

        for (int stream = 0; stream < numStreams; stream++) {
            Iterator<IndexEntry> iterator = diskIndex.iterator(Range.allOf(stream));
            assertIteratorHasAllEntries(stream, numEvents, iterator);
        }
    }

    @Test
    public void full_scan_iterator() {

        int numStreams = 200;
        int numEvents = 500;

        IndexSegment diskIndex = indexWithXStreamsWithYEventsEach(numStreams, numEvents);

        Iterator<IndexEntry> iterator = diskIndex.iterator();

        int count = 0;
        IndexEntry preciousEntry = null;
        while (iterator.hasNext()) {
            IndexEntry current = iterator.next();
            if (preciousEntry != null) {
                assertTrue(current.greaterThan(preciousEntry));
            }
            preciousEntry = current;
            count++;
        }

        assertEquals(numStreams * numEvents, count);
    }

    @Test
    public void write() {
        MemIndex memIndex = new MemIndex();
        memIndex.add(1L, 0, 0);
        memIndex.add(1L, 1, 0);
        memIndex.add(1L, 2, 0);
        memIndex.add(1L, 3, 0);

        IndexSegment idx = IndexSegment.write(memIndex, indexFile);

        assertEquals(2, idx.midpoints.length);

        assertEquals(IndexEntry.of(1L, 0, 0), idx.midpoints[0].key);
        assertEquals(IndexEntry.of(1L, 3, 0), idx.midpoints[idx.midpoints.length - 1].key);

    }

    @Test
    public void load() {

        MemIndex memIndex = new MemIndex();
        memIndex.add(1L, 0, 0);
        memIndex.add(1L, 1, 0);
        memIndex.add(1L, 2, 0);
        memIndex.add(1L, 3, 0);

        IndexSegment idx = IndexSegment.write(memIndex, indexFile);

        IndexSegment found = IndexSegment.load(indexFile);

        assertEquals(2, found.midpoints.length);
        assertEquals(idx.midpoints[0].key, found.midpoints[0].key);

        assertEquals(IndexEntry.of(1L, 0, 0), idx.midpoints[0].key);
        assertEquals(IndexEntry.of(1L, 3, 0), idx.midpoints[idx.midpoints.length - 1].key);

    }

    @Test
    public void when_range_query_gt_index_bounds_return_empty_set() {

        long stream = 1;
        long streamQuery = 2;

        MemIndex memIndex = new MemIndex();
        memIndex.add(stream, 0, 0);
        memIndex.add(stream, 1, 0);
        memIndex.add(stream, 2, 0);
        memIndex.add(stream, 3, 0);

        IndexSegment idx = IndexSegment.write(memIndex, indexFile);

        Range range = Range.of(streamQuery, 0);

        assertEquals(0, idx.stream(range).count());
    }

    @Test
    public void iterator_return_version_in_increasing_order() {

        long stream = 1;
        long streamQuery = 2;

        MemIndex memIndex = new MemIndex();
        memIndex.add(stream, 0, 0);
        memIndex.add(stream, 1, 0);
        memIndex.add(stream, 2, 0);
        memIndex.add(stream, 3, 0);


        IndexSegment idx = IndexSegment.write(memIndex, indexFile);

        Range range = Range.of(streamQuery, 0);

        assertEquals(0, idx.stream(range).count());
    }

    @Test
    public void when_range_query_lt_index_bounds_return_empty_set() {

        long stream = 2;
        long streamQuery = 1;

        MemIndex memIndex = new MemIndex();
        memIndex.add(stream, 0, 0);
        memIndex.add(stream, 1, 0);
        memIndex.add(stream, 2, 0);
        memIndex.add(stream, 3, 0);

        IndexSegment idx = IndexSegment.write(memIndex, indexFile);

        Range range = Range.of(streamQuery, 0);

        assertEquals(0, idx.stream(range).count());
    }

    @Test
    public void when_range_query_in_index_bounds_return_all_matches() {

        long stream = 1;

        MemIndex memIndex = new MemIndex();
        memIndex.add(stream, 0, 0);
        memIndex.add(stream, 1, 0);
        memIndex.add(stream, 2, 0);
        memIndex.add(stream, 3, 0);

        IndexSegment idx = IndexSegment.write(memIndex, indexFile);

        Range range = Range.of(stream, 1, 3);

        Iterator<IndexEntry> it = idx.iterator(range);

        assertTrue(it.hasNext());
        IndexEntry next = it.next();
        assertEquals(stream, next.stream);
        assertEquals(1, next.version);

        assertTrue(it.hasNext());
        next = it.next();
        assertEquals(stream, next.stream);
        assertEquals(2, next.version);

        assertFalse(it.hasNext());

    }

    private void assertIteratorHasAllEntries(long stream, int latestVersion, Iterator<IndexEntry> iterator) {
        int previousVersion = 0;
        int count = 0;
        while (iterator.hasNext()) {
            IndexEntry next = iterator.next();

            assertEquals(stream, next.stream);
            assertEquals(previousVersion + 1, next.version);
            previousVersion = next.version;
            count++;
        }

        assertEquals(latestVersion, previousVersion);
        assertEquals(latestVersion, count);
    }

    private IndexSegment indexWithStreamRanging(int from, int to) {
        //given
        MemIndex memIndex = new MemIndex();
        for (int i = from; i <= to; i++) {
            memIndex.add(i, 1, 0);
        }
        return IndexSegment.write(memIndex, indexFile);
    }

    private IndexSegment indexWithSameStreamWithVersionRanging(long stream, int from, int to) {
        //given
        MemIndex memIndex = new MemIndex();
        for (int i = from; i <= to; i++) {
            memIndex.add(stream, i, 0);
        }
        return IndexSegment.write(memIndex, indexFile);
    }

    private IndexSegment indexWithXStreamsWithYEventsEach(int streamQtd, int eventsPerStream) {
        //given
        MemIndex memIndex = new MemIndex();
        for (int stream = 0; stream < streamQtd; stream++) {
            for (int version = 1; version <= eventsPerStream; version++) {
                memIndex.add(stream, version, 0);
            }
        }
        return IndexSegment.write(memIndex, indexFile);
    }

}