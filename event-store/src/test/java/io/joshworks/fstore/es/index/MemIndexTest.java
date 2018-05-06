package io.joshworks.fstore.es.index;

import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;

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
            if(last != null) {
                assertEquals(last.stream + 1, next.stream);
            }
            last = next;
            count++;

        }

        assertEquals(streams, count);
    }
}
