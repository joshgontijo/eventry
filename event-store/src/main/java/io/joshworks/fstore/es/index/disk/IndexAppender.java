package io.joshworks.fstore.es.index.disk;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.DataReader;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.es.index.Index;
import io.joshworks.fstore.es.index.IndexEntry;
import io.joshworks.fstore.es.index.Range;
import io.joshworks.fstore.es.utils.Iterators;
import io.joshworks.fstore.log.Log;
import io.joshworks.fstore.log.appender.Builder;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.log.appender.Order;
import io.joshworks.fstore.log.appender.naming.ShortUUIDNamingStrategy;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class IndexAppender extends LogAppender<IndexEntry, IndexSegment> implements Index {

    private final int numElements;

    public IndexAppender(Builder<IndexEntry> builder, int numElements) {
        super(builder);
        this.numElements = numElements;
    }

    @Override
    protected IndexSegment createSegment(Storage storage, Serializer<IndexEntry> serializer, DataReader reader) {
        return new IndexSegment(storage, new FixedSizeBlockSerializer<>(serializer, IndexEntry.BYTES), reader, 0, false, directory().toFile(), numElements);
    }

    @Override
    protected IndexSegment openSegment(Storage storage, Serializer<IndexEntry> serializer, DataReader reader, long position, boolean readonly) {
        return new IndexSegment(storage, new FixedSizeBlockSerializer<>(serializer, IndexEntry.BYTES), reader, position, readonly, directory().toFile(), numElements);
    }

    @Override
    public Iterator<IndexEntry> iterator(Range range) {
        List<Iterator<IndexEntry>> iterators = streamSegments(Order.OLDEST).map(Log::iterator).collect(Collectors.toList());
        return Iterators.concat(iterators);
    }

    @Override
    public Stream<IndexEntry> stream(Range range) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator(), Spliterator.ORDERED), false);
    }

    @Override
    public Optional<IndexEntry> get(long stream, int version) {
        Iterator<IndexSegment> segments = segments(Order.NEWEST);
        while (segments.hasNext()) {
            IndexSegment next = segments.next();
            Optional<IndexEntry> fromDisk = next.get(stream, version);
            if (fromDisk.isPresent()) {
                return fromDisk;
            }
        }
        return Optional.empty();
    }

    @Override
    public int version(long stream) {
        Iterator<IndexSegment> segments = segments(Order.NEWEST);
        while (segments.hasNext()) {
            IndexSegment segment = segments.next();
            int version = segment.version(stream);
            if (version > 0) {
                return version;
            }
        }
        return 0;
    }

    @Override
    public Iterator<IndexEntry> iterator() {
        List<Iterator<IndexEntry>> segments = streamSegments(Order.NEWEST).map(Log::iterator).collect(Collectors.toList());
        return Iterators.concat(segments);
    }

    public static class IndexNaming extends ShortUUIDNamingStrategy {
        @Override
        public String prefix() {
            return "index-" + super.prefix();
        }
    }

}
