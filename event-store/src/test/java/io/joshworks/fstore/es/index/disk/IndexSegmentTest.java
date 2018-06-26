package io.joshworks.fstore.es.index.disk;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.Mode;
import io.joshworks.fstore.core.io.RafStorage;
import io.joshworks.fstore.es.Utils;
import io.joshworks.fstore.es.index.IndexEntry;
import io.joshworks.fstore.es.index.Range;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.reader.HeaderLengthDataReader;
import io.joshworks.fstore.log.segment.Type;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class IndexSegmentTest {

    private File segmentFile;
    private File indexDir;

    private IndexSegment segment;
    private static final int NUMBER_OF_ELEMENTS = 1000000; //bloom filter

    @Before
    public void setUp() {
        indexDir = Utils.testFolder();
        segmentFile = new File(indexDir, "test-index");
        segment = open(segmentFile);
    }

    @After
    public void tearDown() throws Exception {
        IOUtils.closeQuietly(segment);
        Files.delete(segmentFile.toPath());
    }

    public IndexSegment open(File location) {
        long size = location.length() == 0 ? 1048576 : location.length();
        return new IndexSegment(
                new RafStorage(location, size, Mode.READ_WRITE),
                new FixedSizeBlockSerializer<>(new IndexEntrySerializer(), IndexEntry.BYTES),
                new HeaderLengthDataReader(),
                Type.LOG_HEAD,
                indexDir,
                NUMBER_OF_ELEMENTS);
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
        diskIndex.close();
        try (IndexSegment loaded = open(segmentFile)) {

            //then
            assertEquals(diskIndex.midpoints.size(), loaded.midpoints.size());

            assertNotNull(diskIndex.midpoints.first());
            assertNotNull(diskIndex.midpoints.last());

            assertEquals(diskIndex.midpoints.first(), loaded.midpoints.first());
            assertEquals(diskIndex.midpoints.last(), loaded.midpoints.last());
        }
    }

    @Test
    public void loaded_segmentIndex_has_the_same_1000000_midpoints() {
        //given
        IndexSegment diskIndex = indexWithStreamRanging(1, 1000000);

        //when
        diskIndex.close();
        try (IndexSegment loaded = open(segmentFile)) {
            //then
            assertEquals(diskIndex.midpoints.size(), loaded.midpoints.size());
            assertEquals(diskIndex.midpoints.first(), loaded.midpoints.first());
            assertEquals(diskIndex.midpoints.last(), loaded.midpoints.last());
        }
    }

    @Test
    public void loaded_segmentIndex_has_the_same_filter_items() {
        //given

        IndexSegment diskIndex = indexWithStreamRanging(1, 1000000);

        //when
        assertFalse(diskIndex.filter.contains(0L));
        assertFalse(diskIndex.filter.contains(1000001L));
        diskIndex.close();
        try (IndexSegment loaded = open(segmentFile)) {
            //then

            assertEquals(diskIndex.filter, loaded.filter);

            assertTrue(loaded.filter.contains(1L));
            assertFalse(loaded.filter.contains(-1L));

            loaded.append(IndexEntry.of(999999999L, 1, 0));
            assertTrue(loaded.filter.contains(999999999L));

        }
    }

    @Test
    public void reopen_loads_all_four_entries() {

        //given
        segment.append(IndexEntry.of(1L, 1, 0));
        segment.append(IndexEntry.of(1L, 2, 0));
        segment.append(IndexEntry.of(1L, 3, 0));
        segment.append(IndexEntry.of(1L, 4, 0));


        //when
        segment.close();
        try (IndexSegment opened = open(segmentFile)) {
            //then
            long items = opened.stream().count();
            assertEquals(4, items);

        }
    }

    @Test
    public void reopened_segment_returns_correct_data() {

        //given
        IndexEntry e1 = IndexEntry.of(1L, 1, 0);
        IndexEntry e2 = IndexEntry.of(1L, 2, 0);
        IndexEntry e3 = IndexEntry.of(1L, 3, 0);
        IndexEntry e4 = IndexEntry.of(1L, 4, 0);

        segment.append(e1);
        segment.append(e2);
        segment.append(e3);
        segment.append(e4);

        segment.roll(1);

        Optional<IndexEntry> ie = segment.get(1L, 1);
        assertTrue(ie.isPresent());
        assertEquals(e1, ie.get());

        //when
        segment.close();
        try (IndexSegment opened = open(segmentFile)) {
            //then
            long items = opened.stream().count();
            assertEquals(4, items);

            Optional<IndexEntry> found = opened.get(1L, 1);
            assertTrue(found.isPresent());
            assertEquals(e1, found.get());

            Stream<IndexEntry> stream = opened.stream(Range.allOf(1L));
            assertEquals(4, stream.count());
        }
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
    public void version_with_index_full_of_the_same_stream() {

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
    public void range_with_multiple_streams_with_single_version() {
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
    public void range_with_multiple_streams_with_multiple_versions() {
        //given
        int startStream = 1;
        int endStream = 10000;
        int numVersions = 500;
        IndexSegment diskIndex = indexWithXStreamsWithYEventsEach(endStream, numVersions);

        //when
        for (int i = startStream; i < endStream; i++) {
            long size = diskIndex.stream(Range.allOf(i)).count();

            //then
            assertEquals("Failed on position " + i, numVersions, size);
        }
    }

    @Test
    public void stream_version_with_index_10000_streams() {

        //given
        int startStream = 1;
        int endStream = 10000;
        IndexSegment diskIndex = indexWithStreamRanging(startStream, endStream);

        //when
        for (int i = startStream; i <= endStream; i++) {
            int version = diskIndex.version(i);
            //then
            assertEquals("Failed on iteration " + i, 1, version);
        }
    }

    @Test
    public void stream_version_with_index_100000_streams_and_5_versions_each() {

        //given
        int startStream = 1;
        int endStream = 10000;
        int numVersions = 10;
        IndexSegment diskIndex = indexWithXStreamsWithYEventsEach(endStream, numVersions);

        //when
        for (int i = startStream; i < endStream; i++) {
            int version = diskIndex.version(i);
            //then
            assertEquals("Failed on iteration " + i, numVersions, version);
        }
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
        IndexEntry previousEntry = null;
        while (iterator.hasNext()) {
            IndexEntry current = iterator.next();
            if (previousEntry != null) {
                assertTrue(current.greaterThan(previousEntry));
            }
            previousEntry = current;
            count++;
        }

        assertEquals(numStreams * numEvents, count);
    }

    @Test
    public void when_range_query_gt_index_bounds_return_empty_set() {

        //given
        long stream = 1;
        long streamQuery = 2;

        segment.append(IndexEntry.of(stream, 1, 0));
        segment.append(IndexEntry.of(stream, 2, 0));
        segment.append(IndexEntry.of(stream, 3, 0));
        segment.append(IndexEntry.of(stream, 4, 0));

        //when
        Range range = Range.of(streamQuery, 1);
        Stream<IndexEntry> items = segment.stream(range);

        //then
        assertEquals(0, items.count());
    }

    @Test
    public void iterator_return_version_in_increasing_order() {

        long stream = 1;

        segment.append(IndexEntry.of(stream, 1, 0));
        segment.append(IndexEntry.of(stream, 2, 0));
        segment.append(IndexEntry.of(stream, 3, 0));
        segment.append(IndexEntry.of(stream, 4, 0));

        segment.roll(1);

        Range range = Range.of(stream, 1);

        assertEquals(4, segment.stream(range).count());

        LogIterator<IndexEntry> iterator = segment.iterator();

        int lastVersion = 0;
        while (iterator.hasNext()) {
            IndexEntry next = iterator.next();
            assertEquals(lastVersion + 1, next.version);
            lastVersion = next.version;
        }
    }

    @Test
    public void when_range_query_lt_index_bounds_return_empty_set() {

        long stream = 2;
        long streamQuery = 1;


        segment.append(IndexEntry.of(stream, 1, 0));
        segment.append(IndexEntry.of(stream, 2, 0));
        segment.append(IndexEntry.of(stream, 3, 0));
        segment.append(IndexEntry.of(stream, 4, 0));

        segment.roll(1);

        Range range = Range.of(streamQuery, 1);

        assertEquals(0, segment.stream(range).count());
    }

    @Test
    public void when_range_query_in_index_bounds_return_all_matches() {

        long stream = 1;

        segment.append(IndexEntry.of(stream, 1, 0));
        segment.append(IndexEntry.of(stream, 2, 0));
        segment.append(IndexEntry.of(stream, 3, 0));
        segment.append(IndexEntry.of(stream, 4, 0));

        segment.roll(1);

        Range range = Range.of(stream, 1, 3);

        Iterator<IndexEntry> it = segment.iterator(range);

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


    @Test
    public void version_of_large_items() {

        //given
        int numStreams = 1000000;
        IndexSegment testSegment = indexWithStreamRanging(0, numStreams);

        for (int i = 0; i < numStreams; i++) {
            //when
            int version = testSegment.version(i);

            //then
            assertEquals("Failed on iteration " + i, 1, version);
        }

    }

    @Test
    public void version_of_inexistent_stream_returns_zero() {

        //given
        int numStreams = 1000;
        int numOfQueries = 100000;
        IndexSegment testSegment = indexWithStreamRanging(0, numStreams);

        for (int i = numStreams + 1; i < numOfQueries; i++) {
            //when
            int version = testSegment.version(i);

            //then
            assertEquals("Failed on iteration " + i, 0, version);
        }

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
        for (int i = from; i <= to; i++) {
            segment.append(IndexEntry.of(i, 1, 0));
        }
        segment.roll(1);
        return segment;
    }

    private IndexSegment indexWithSameStreamWithVersionRanging(long stream, int from, int to) {
        for (int i = from; i <= to; i++) {
            segment.append(IndexEntry.of(stream, i, 0));
        }
        segment.roll(1);
        return segment;
    }

    private IndexSegment indexWithXStreamsWithYEventsEach(int streamQtd, int eventsPerStream) {
        //given
        for (int stream = 0; stream < streamQtd; stream++) {
            for (int version = 1; version <= eventsPerStream; version++) {
                segment.append(IndexEntry.of(stream, version, 0));
            }
        }
        segment.roll(1);
        return segment;
    }

}