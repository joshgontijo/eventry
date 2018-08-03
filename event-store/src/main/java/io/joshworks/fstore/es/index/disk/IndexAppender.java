package io.joshworks.fstore.es.index.disk;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.DataReader;
import io.joshworks.fstore.core.io.Mode;
import io.joshworks.fstore.core.io.RafStorage;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.util.Iterators;
import io.joshworks.fstore.es.index.Index;
import io.joshworks.fstore.es.index.IndexEntry;
import io.joshworks.fstore.es.index.Range;
import io.joshworks.fstore.log.appender.Config;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.log.appender.Order;
import io.joshworks.fstore.log.appender.SegmentFactory;
import io.joshworks.fstore.log.appender.naming.ShortUUIDNamingStrategy;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IndexAppender extends LogAppender<IndexEntry, IndexSegment> implements Index {

    public IndexAppender(Config<IndexEntry> config, int numElements) {
        super(config, new IndexSegmentFactory(config.directory, numElements));
    }

    @Override
    public Iterator<IndexEntry> iterator(Range range) {
        List<Iterator<IndexEntry>> iterators = streamSegments(Order.OLDEST)
                .map(idxSeg -> idxSeg.iterator(range))
                .collect(Collectors.toList());

        return Iterators.concat(iterators);
    }

    @Override
    public Stream<IndexEntry> stream(Range range) {
        return Iterators.stream(iterator(range));
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
            if (version >= 0) {
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

    private static class IndexSegmentFactory implements SegmentFactory<IndexEntry, IndexSegment> {

        private final File directory;
        private final int numElements;

        private IndexSegmentFactory(File directory, int numElements) {
            this.directory = directory;
            this.numElements = numElements;
        }

        @Override
        public IndexSegment createOrOpen(Storage storage, Serializer<IndexEntry> serializer, DataReader reader, Type type) {
            return new IndexSegment(storage, new FixedSizeBlockSerializer<>(serializer, IndexEntry.BYTES), reader, type, directory, numElements);
        }
    }

}
