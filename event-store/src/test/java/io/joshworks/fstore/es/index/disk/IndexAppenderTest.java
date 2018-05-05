package io.joshworks.fstore.es.index.disk;

import io.joshworks.fstore.es.index.IndexEntry;
import io.joshworks.fstore.log.appender.Builder;
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

    @Before
    public void setUp() {

        Builder<IndexEntry> builder = LogAppender
                .builder(new File("J:\\EVENT-STORE\\" + UUID.randomUUID().toString().substring(0,8)),
                        new IndexEntrySerializer()).mmap();
        appender = new IndexAppender(builder);
    }

    @After
    public void tearDown() {
       appender.close();
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
        Iterator<IndexEntry> iterator = appender.iterator();
        while (iterator.hasNext()) {
            IndexEntry next = iterator.next();
            last = next;
//            System.out.println(next);
            count++;
        }

        System.out.println(last);
        System.out.println("READ " + (System.currentTimeMillis() - start));
        assertEquals(1000000, count);

//        Iterator<IndexEntry> iterator1 = appender.current().iterator(Range.of(123, 250, 260));
//        while (iterator1.hasNext()) {
//            IndexEntry next = iterator1.next();
//            System.out.println(next);
//        }

    }
}