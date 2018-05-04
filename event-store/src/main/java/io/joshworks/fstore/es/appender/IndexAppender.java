package io.joshworks.fstore.es.appender;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.DataReader;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.es.index.IndexEntry;
import io.joshworks.fstore.es.index.MemIndex;
import io.joshworks.fstore.es.index.testing.LRUCache;
import io.joshworks.fstore.log.appender.Builder;
import io.joshworks.fstore.log.appender.LogAppender;

import java.util.Iterator;

public class IndexAppender extends LogAppender<FixedSizeEntryBlock<IndexEntry>, IndexSegment> {

    private final LRUCache streamVersion = new LRUCache(1000);

    protected IndexAppender(Builder<FixedSizeEntryBlock<IndexEntry>> builder) {
        super(builder);
    }

    @Override
    protected IndexSegment createSegment(Storage storage, Serializer<FixedSizeEntryBlock<IndexEntry>> serializer, DataReader reader) {
        return new IndexSegment(storage, serializer, reader, 0, false, directory().toFile());
    }

    @Override
    protected IndexSegment openSegment(Storage storage, Serializer<FixedSizeEntryBlock<IndexEntry>> serializer, DataReader reader, long position, boolean readonly) {
        return new IndexSegment(storage, serializer, reader, position, readonly, directory().toFile());
    }


    public void write(MemIndex memIndex) {
        current().write(memIndex);
        current().roll();
    }

    //TODO delete me
    public Iterator<IndexEntry> iterator() {
        return current().iterator();
    }


}
