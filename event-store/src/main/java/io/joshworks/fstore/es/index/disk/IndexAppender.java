package io.joshworks.fstore.es.index.disk;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.DataReader;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.es.index.Index;
import io.joshworks.fstore.es.index.IndexEntry;
import io.joshworks.fstore.es.index.Range;
import io.joshworks.fstore.es.utils.Iterators;
import io.joshworks.fstore.log.appender.Builder;
import io.joshworks.fstore.log.appender.LogAppender;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class IndexAppender extends LogAppender<IndexEntry, IndexSegment> implements Index {

    public IndexAppender(Builder<IndexEntry> builder) {
        super(builder);
    }

    @Override
    protected IndexSegment createSegment(Storage storage, Serializer<IndexEntry> serializer, DataReader reader) {
        return new IndexSegment(storage, new FixedSizeBlockSerializer<>(serializer, IndexEntry.BYTES), reader, 0, false, directory().toFile());
    }

    @Override
    protected IndexSegment openSegment(Storage storage, Serializer<IndexEntry> serializer, DataReader reader, long position, boolean readonly) {
        return new IndexSegment(storage, new FixedSizeBlockSerializer<IndexEntry>(serializer, IndexEntry.BYTES), reader, position, readonly, directory().toFile());
    }

    @Override
    public Iterator<IndexEntry> iterator(Range range) {
        List<Iterator<IndexEntry>> iterators = new ArrayList<>();
        Iterator<IndexSegment> iter = segmentsReverse();
        while (iter.hasNext()) {
            IndexSegment next = iter.next();
            iterators.add(next.iterator(range));
        }
        return Iterators.concat(iterators);

    }

    @Override
    public Stream<IndexEntry> stream(Range range) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator(), Spliterator.ORDERED), false);
    }

    @Override
    public Optional<IndexEntry> get(long stream, int version) {
        Iterator<IndexSegment> iter = segmentsReverse();
        while (iter.hasNext()) {
            IndexSegment next = iter.next();
            Optional<IndexEntry> fromDisk = next.get(stream, version);
            if (fromDisk.isPresent()) {
                return fromDisk;
            }
        }
        return Optional.empty();
    }

    @Override
    public int version(long stream) {
        Iterator<IndexSegment> reverseSegments = segmentsReverse();

        while(reverseSegments.hasNext()) {
            IndexSegment segment = reverseSegments.next();
            int version = segment.version(stream);
            if(version > 0) {
                return version;
            }
        }
        return 0;
    }

    @Override
    public Iterator<IndexEntry> iterator() {
        List<Iterator<IndexEntry>> iterators = new ArrayList<>();
        Iterator<IndexSegment> iter = segmentsReverse();
        while (iter.hasNext()) {
            IndexSegment next = iter.next();
            iterators.add(next.iterator());
        }
        return Iterators.concat(iterators);
    }

}
