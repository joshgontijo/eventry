package io.joshworks.fstore.es.index;

import io.joshworks.fstore.log.PollingSubscriber;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class MemIndexTest {

    private MemIndex index = new MemIndex();


    @Test
    public void stream_return_same_number_of_events_of_same_stream() {

        int numEntries = 10000;
        long stream = 123;
        for (int i = 1; i <= numEntries; i++) {
            index.add(IndexEntry.of(stream, i, 0));
        }

        assertEquals(numEntries, index.stream().count());
    }

    @Test
    public void stream_return_same_number_of_events_of_multiple_streams() {

        //given
        int versions = 1000;
        int streams = 50;
        for (int stream = 0; stream < streams; stream++) {
            for (int version = 1; version <= versions; version++) {
                index.add(IndexEntry.of(stream, version, 0));
            }
        }

        for (int stream = 0; stream < streams; stream++) {
            //when
            long count = index.stream(Range.allOf(stream)).count();
            //then
            assertEquals(versions, count);
        }
    }

    @Test
    public void event_number_are_in_increasing_order_by_version() {

        //given
        int versions = 10000;
        long streams = 123;
        for (int stream = 0; stream < streams; stream++) {
            for (int version = 1; version <= versions; version++) {
                index.add(IndexEntry.of(stream, version, 0));
            }
        }

        for (int stream = 0; stream < streams; stream++) {
            int lastVersion = 0;

            //when
            Iterator<IndexEntry> iterator = index.iterator(Range.allOf(stream));
            while (iterator.hasNext()) {
                IndexEntry indexEntry = iterator.next();
                //then
                assertEquals(stream, indexEntry.stream);
                assertEquals(lastVersion + 1, indexEntry.version);
                lastVersion = indexEntry.version;
            }
            assertEquals(versions, lastVersion);
        }
    }

    @Test
    public void event_number_are_in_increasing_order_by_version_with_multiple_streams() {

        //given
        int versions = 1000;
        int streams = 50;
        for (int stream = 0; stream < streams; stream++) {
            for (int version = 1; version <= versions; version++) {
                index.add(IndexEntry.of(stream, version, 0));
            }
        }


        //when
        for (int stream = 0; stream < streams; stream++) {
            int lastVersion = 0;

            Iterator<IndexEntry> iterator = index.iterator(Range.allOf(stream));
            while (iterator.hasNext()) {
                IndexEntry indexEntry = iterator.next();
                //then
                assertEquals(stream, indexEntry.stream);
                assertEquals(lastVersion + 1, indexEntry.version);
                lastVersion = indexEntry.version;
            }

            assertEquals(versions, lastVersion);
        }
    }

    @Test
    public void iterator() {
        //given
        int streams = 50;
        for (int stream = 0; stream < streams; stream++) {
            index.add(IndexEntry.of(stream, 1, 0));
        }

        int count = 0;
        IndexEntry last = null;
        for (IndexEntry next : index) {
            if (last != null) {
                assertEquals(last.stream + 1, next.stream);
            }
            last = next;
            count++;

        }

        assertEquals(streams, count);
    }

    @Test
    public void version_returns_the_latest_version() {

        //given
        long stream = 0;
        index.add(IndexEntry.of(stream, 1, 0));
        index.add(IndexEntry.of(stream, 2, 0));
        index.add(IndexEntry.of(stream, 3, 0));

        //when
        int version = index.version(stream);

        //then
        assertEquals(3, version);
    }

    @Test
    public void version_returns_the_latest_version_with_multiple_streams() {

        //given
        long stream = 0;
        long anotherStream = 1;
        index.add(IndexEntry.of(stream, 1, 0));
        index.add(IndexEntry.of(stream, 2, 0));
        index.add(IndexEntry.of(stream, 3, 0));

        index.add(IndexEntry.of(anotherStream, 1, 0));
        index.add(IndexEntry.of(anotherStream, 2, 0));
        index.add(IndexEntry.of(anotherStream, 3, 0));

        //when
        int version = index.version(stream);
        int anotherStreamVersion = index.version(anotherStream);

        //then
        assertEquals(3, version);
        assertEquals(3, anotherStreamVersion);
    }

    @Test
    public void size_returns_number_of_items_in_the_map() {

        //given
        index.add(IndexEntry.of(0, 1, 0));
        index.add(IndexEntry.of(0, 2, 0));
        index.add(IndexEntry.of(0, 3, 0));

        //when
        int size = index.size();

        //then
        assertEquals(3, size);
    }

    @Test
    public void no_entries_returns_empty() {
        //given
        //empty index

        //when
        boolean empty = index.isEmpty();

        //then
        assertTrue(empty);
    }

    @Test
    public void not_empty() {
        //given
        index.add(IndexEntry.of(0, 1, 0));

        //when
        boolean empty = index.isEmpty();

        //then
        assertFalse(empty);
    }

    @Test
    public void close_doesnt_clear_the_map_and_size() {
        //given
        index.add(IndexEntry.of(0, 1, 0));
        index.close();

        //when
        boolean empty = index.isEmpty();
        int size = index.size();

        //then
        assertFalse(empty);
        assertEquals(1, size);
    }

    @Test
    public void stream_range_returns_right_number_of_entries() {

        //given
        int stream = 0;
        int versions = 1000;
        for (int i = 1; i < versions; i++) {
            index.add(IndexEntry.of(stream, i, 0));
        }

        //when
        Stream<IndexEntry> dataStream = index.stream(Range.of(stream, 500));

        //then
        assertEquals(500, dataStream.count());

    }

    @Test
    public void stream_range_returns_data_within_bounds() {

        //given
        int stream = 0;
        int versions = 1000;
        for (int i = 1; i < versions; i++) {
            index.add(IndexEntry.of(stream, i, 0));
        }

        //when
        int versionStart = 500;
        Iterator<IndexEntry> iterator = index.iterator(Range.of(stream, versionStart));

        //then
        int lastVersion = versionStart - 1;
        while (iterator.hasNext()) {
            IndexEntry next = iterator.next();
            assertEquals(stream, next.stream);
            assertTrue(next.version >= versionStart);
            assertEquals(lastVersion + 1, next.version);
            lastVersion = next.version;
        }
    }

    @Test
    public void stream_range_higher_than_size_returns_data_within_bounds() {

        //given
        int stream = 0;
        int versions = 1000;
        for (int i = 1; i < versions; i++) {
            index.add(IndexEntry.of(stream, i, 0));
        }

        //when
        int versionStart = 500;
        Iterator<IndexEntry> iterator = index.iterator(Range.of(stream, versionStart, 999999));

        //then
        int lastVersion = versionStart - 1;
        while (iterator.hasNext()) {
            IndexEntry next = iterator.next();
            assertEquals(stream, next.stream);
            assertTrue(next.version >= versionStart);
            assertEquals(lastVersion + 1, next.version);
            lastVersion = next.version;
        }
    }

    @Test
    public void iterator_with_high_limit_returns_data_within_bounds() {

        //given
        int stream = 0;
        int versions = 1000;
        for (int i = 1; i < versions; i++) {
            index.add(IndexEntry.of(stream, i, 0));
        }

        //when
        int versionStart = 500;
        Iterator<IndexEntry> iterator = index.iterator(Range.of(stream, versionStart, versionStart + 1));

        assertTrue(iterator.hasNext());
        assertEquals(IndexEntry.of(stream, versionStart, 0), iterator.next());
        assertFalse(iterator.hasNext());

    }

    @Test
    public void get_returns_the_specific_entry() {

        //given
        IndexEntry ie1 = IndexEntry.of(0, 1, 0);
        IndexEntry ie2 = IndexEntry.of(0, 2, 0);
        IndexEntry ie3 = IndexEntry.of(0, 3, 0);
        IndexEntry ie4 = IndexEntry.of(1, 1, 0);
        IndexEntry ie5 = IndexEntry.of(1, 2, 0);
        IndexEntry ie6 = IndexEntry.of(1, 3, 0);

        index.add(ie1);
        index.add(ie2);
        index.add(ie3);
        index.add(ie4);
        index.add(ie5);
        index.add(ie6);


        //when
        Optional<IndexEntry> f1 = index.get(0, 1);
        Optional<IndexEntry> f2 = index.get(0, 2);
        Optional<IndexEntry> f3 = index.get(0, 3);
        Optional<IndexEntry> f4 = index.get(1, 1);
        Optional<IndexEntry> f5 = index.get(1, 2);
        Optional<IndexEntry> f6 = index.get(1, 3);


        //then
        assertTrue(f1.isPresent());
        assertEquals(ie1, f1.get());

        assertTrue(f2.isPresent());
        assertEquals(ie2, f2.get());

        assertTrue(f3.isPresent());
        assertEquals(ie3, f3.get());

        assertTrue(f4.isPresent());
        assertEquals(ie4, f4.get());

        assertTrue(f5.isPresent());
        assertEquals(ie5, f5.get());

        assertTrue(f6.isPresent());
        assertEquals(ie6, f6.get());
    }

    @Test
    public void non_existent_entry_returns_empty_optional() {

        //given
        index.add(IndexEntry.of(0, 1, 0));

        //when
        Optional<IndexEntry> indexEntry = index.get(1, 1);

        //then
        assertFalse(indexEntry.isPresent());
    }

    @Test
    public void iterator_without_range_returns_everything() {

        //given
        index.add(IndexEntry.of(0, 1, 0));
        index.add(IndexEntry.of(0, 2, 0));
        index.add(IndexEntry.of(0, 3, 0));
        index.add(IndexEntry.of(1, 1, 0));
        index.add(IndexEntry.of(1, 2, 0));
        index.add(IndexEntry.of(1, 3, 0));

        //when
        Stream<IndexEntry> iterator = index.stream();

        //then
        assertEquals(6, iterator.count());
    }

    @Test
    public void version_is_minus_one_for_non_existing_stream() {
        int version = index.version(1234);
        assertEquals(IndexEntry.NO_VERSION, version);
    }

    @Test
    public void poller_poll_returns_all_data() throws IOException, InterruptedException {

        int entries = 500;
        for (int i = 0; i < entries; i++) {
            index.add(IndexEntry.of(i, 0, 0));
        }

        try (PollingSubscriber<IndexEntry> poller = index.poller()) {
            for (int i = 0; i < entries; i++) {
                IndexEntry poll = poller.poll();
                assertNotNull(poll);
                assertEquals(i, poll.stream);
            }
        }
    }

    @Test
    public void endOfLog_when_is_closed_and_read_all_entries() throws IOException, InterruptedException {

        index.add(IndexEntry.of(1, 0, 0));

        PollingSubscriber<IndexEntry> poller = index.poller();
        assertFalse(poller.endOfLog());

        poller.poll();

        assertFalse(poller.endOfLog());
        poller.close();
        assertTrue(poller.endOfLog());
    }

    @Test
    public void headOfLog_when_there_are_no_more_entries() throws InterruptedException {

        index.add(IndexEntry.of(1, 0, 0));

        PollingSubscriber<IndexEntry> poller = index.poller();
        assertFalse(poller.headOfLog());

        poller.poll();
        assertTrue(poller.headOfLog());
    }
}
