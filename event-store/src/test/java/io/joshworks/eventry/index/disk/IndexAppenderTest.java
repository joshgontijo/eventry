package io.joshworks.eventry.index.disk;

import io.joshworks.eventry.Utils;
import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.Config;
import io.joshworks.fstore.log.appender.LogAppender;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Iterator;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class IndexAppenderTest {

    private IndexAppender appender;
    private File location;

    @Before
    public void setUp() {
        location = new File("J:\\EVENT-STORE\\" + UUID.randomUUID().toString().substring(0, 8));
        Config<IndexEntry> config = LogAppender.builder(location, new IndexEntrySerializer()).disableCompaction();
        appender = new IndexAppender(config, 10000, Codec.noCompression());
    }

    @After
    public void tearDown() {
        appender.close();
        Utils.tryDelete(location);
    }

    @Test
    public void entries_are_returned_from_multiple_segments() {

        int entriesPerSegment = 10;
        int numSegments = 10;

        int stream = 0;
        for (int i = 0; i < entriesPerSegment; i++) {
            for (int x = 0; x < numSegments; x++) {
                appender.append(IndexEntry.of(stream++, 1, 0));
            }
            appender.roll();
        }
        appender.flush();

        int found = 0;
        IndexEntry last = null;

        LogIterator<IndexEntry> iterator = appender.iterator(Direction.FORWARD);
        while(iterator.hasNext()) {
            IndexEntry next = iterator.next();
            found++;
            if (last != null) {
                assertEquals(last.stream + 1, next.stream);
            }
            last = next;
        }

        assertEquals(entriesPerSegment * numSegments, found);

    }

    @Test
    public void write() {

        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            appender.append(IndexEntry.of(i, 1, 0));
        }
        appender.flush();
        System.out.println("WRITE " + (System.currentTimeMillis() - start));


        start = System.currentTimeMillis();
        int count = 0;
        IndexEntry last = null;
        Iterator<IndexEntry> iterator = appender.iterator(Direction.FORWARD);
        while (iterator.hasNext()) {
            IndexEntry next = iterator.next();
            last = next;
            count++;
        }

        System.out.println(last);
        System.out.println("READ " + (System.currentTimeMillis() - start));
        assertEquals(1000000, count);

    }

    @Test
    public void version_with_multiple_segments_returns_correct_version() {

        //given
        int streams = 1000000;
        int numSegments = 2;
        int itemsPerSegment = streams / numSegments;
        for (int i = 0; i < streams; i++) {
            appender.append(IndexEntry.of(i, 1, 0));
            if(i % itemsPerSegment == 0) {
                appender.roll();
            }
        }
        appender.flush();

        long start = System.currentTimeMillis();
        for (int i = 0; i < streams; i++) {

            int version = appender.version(i);
            if(i % 100000 == 0) {
                System.out.println((System.currentTimeMillis() - start));
                start = System.currentTimeMillis();
            }
            assertEquals("Failed on iteration " + i, 1, version);

        }
    }

    @Test
    public void version_is_minus_one_for_non_existing_stream() {
        int version = appender.version(1234);
        assertEquals(IndexEntry.NO_VERSION, version);
    }
}