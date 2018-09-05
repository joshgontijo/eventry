package io.joshworks.fstore.es;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.es.index.IndexEntry;
import io.joshworks.fstore.es.index.Range;
import io.joshworks.fstore.es.index.TableIndex;
import io.joshworks.fstore.es.log.EventLog;
import io.joshworks.fstore.es.log.EventRecord;
import io.joshworks.fstore.es.log.EventSerializer;
import io.joshworks.fstore.es.stream.StreamInfo;
import io.joshworks.fstore.es.stream.StreamMetadata;
import io.joshworks.fstore.es.stream.StreamMetadataSerializer;
import io.joshworks.fstore.es.stream.Streams;
import io.joshworks.fstore.es.utils.StringUtils;
import io.joshworks.fstore.es.utils.Tuple;
import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.PollingSubscriber;
import io.joshworks.fstore.log.appender.LogAppender;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class EventStore implements Closeable {

    //TODO expose
    private static final int LRU_CACHE_SIZE = 1000000;

    private final TableIndex index;
    private final Streams streams;
    private final EventLog eventLog;

    private EventStore(File rootDir) {
        this.eventLog = new EventLog(LogAppender.builder(rootDir, new EventSerializer()).segmentSize((int) Size.MEGABYTE.toBytes(200)).disableCompaction());
        this.index = new TableIndex(rootDir);
        this.streams = new Streams(LRU_CACHE_SIZE, index::version);
        this.loadStreams();
    }

    public static EventStore open(File rootDir) {
        return new EventStore(rootDir);
    }

    //TODO relies on read backwards
    private void loadIndex() {

    }

    private void loadStreams() {
        final Serializer<StreamMetadata> serializer = new StreamMetadataSerializer();
        fromStream(EventRecord.System.STREAMS_STREAM).forEach(event -> {
            StreamMetadata streamMetadata = serializer.fromBytes(ByteBuffer.wrap(event.data));
            if (EventRecord.System.STREAM_DELETED_TYPE.equals(event.type)) {
                streams.remove(streamMetadata.hash);
            } else {
                streams.create(streamMetadata);
            }
        });
    }

    public Iterator<IndexEntry> keys() {
        throw new UnsupportedOperationException("TODO");
    }

    public void createStream(String name) {
        createStream(name, -1, -1);
    }

    public void createStream(String name, int maxCount, long maxAge) {
        createStream(name, maxCount, maxAge, new HashMap<>(), new HashMap<>());
    }

    public StreamMetadata createStream(String name, int maxCount, long maxAge, Map<String, Integer> permissions, Map<String, String> metadata) {
        long hash = streams.hashOf(name);
        StreamMetadata streamMetadata = new StreamMetadata(name, hash, System.currentTimeMillis(), maxAge, maxCount, permissions, metadata);

        EventRecord eventRecord = EventRecord.System.streamCreatedRecord(streamMetadata);
        this.append(eventRecord);
        streams.create(streamMetadata);

        return streamMetadata;
    }

    public List<StreamInfo> streamsMetadata() {
        return streams.all().stream().map(meta -> {
            int version = streams.version(meta.hash);
            return StreamInfo.from(meta, version);
        }).collect(Collectors.toList());
    }

    public Optional<StreamInfo> streamMetadata(String stream) {
        long streamHash = streams.hashOf(stream);
        return streams.get(streamHash).map(meta -> {
            int version = streams.version(meta.hash);
            return StreamInfo.from(meta, version);
        });
    }

    public LogIterator<EventRecord> fromStreamIter(String stream) {
        return fromStreamIter(stream, Range.START_VERSION);
    }

    public Stream<EventRecord> fromStream(String stream) {
        return fromStream(stream, Range.START_VERSION);
    }

    public LogIterator<EventRecord> fromStreamIter(String stream, int versionInclusive) {
        long streamHash = streams.hashOf(stream);
        LogIterator<IndexEntry> addresses = index.iterator(Range.of(streamHash, versionInclusive));
        addresses = withMaxCountFilter(streamHash, addresses);
        return withMaxAgeFilter(Set.of(streamHash), new SingleStreamIterator(addresses, this));
    }

    public Stream<EventRecord> fromStream(String stream, int versionInclusive) {
        LogIterator<EventRecord> iterator = fromStreamIter(stream, versionInclusive);
        return Iterators.stream(iterator);
    }

    public Stream<EventRecord> zipStreams(Set<String> streams) {
        LogIterator<EventRecord> iterator = zipStreamsIter(streams);
        return Iterators.stream(iterator);
    }

    public LogIterator<EventRecord> zipStreamsIter(String streamPrefix) {
        Set<String> eventStreams = streams.streamMatching(streamPrefix);
        return zipStreamsIter(eventStreams);
    }

    public Stream<EventRecord> zipStreams(String streamPrefix) {
        return Iterators.stream(zipStreamsIter(streamPrefix));
    }

    public LogIterator<EventRecord> zipStreamsIter(Set<String> streamNames) {
        Set<String> uniqueStreams = new LinkedHashSet<>(streamNames);

        List<LogIterator<IndexEntry>> indexes = new ArrayList<>(streamNames.size());
        Map<Long, String> mappings = new HashMap<>();
        Set<Long> hashes = new HashSet<>();
        for (String stream : uniqueStreams) {
            if (stream == null || stream.isEmpty()) {
                throw new IllegalArgumentException("Stream cannot empty");
            }
            long streamHash = streams.hashOf(stream);
            hashes.add(streamHash);
            LogIterator<IndexEntry> indexStream = index.iterator(Range.allOf(streamHash));

            indexes.add(indexStream);
            mappings.put(streamHash, stream);
        }

        return withMaxAgeFilter(hashes, new MultiStreamIterator(mappings, indexes, eventLog));
    }

    public Stream<Stream<EventRecord>> fromStreams(Set<String> streams) {
        return streams.stream().map(this::fromStream);
    }

    public Map<String, Stream<EventRecord>> fromStreamsMapped(Set<String> streams) {
        return streams.stream()
                .map(stream -> Tuple.of(stream, fromStream(stream)))
                .collect(Collectors.toMap(Tuple::a, Tuple::b));
    }

    public int version(String stream) {
        long streamHash = streams.hashOf(stream);
        return streams.version(streamHash);
    }

    public LogIterator<EventRecord> fromAllIter() {
        return eventLog.scanner();
    }

    //Won't return the stream in the event !
    public Stream<EventRecord> fromAll() {
        return eventLog.stream();
    }

    public EventRecord linkTo(String stream, EventRecord event) {
        if (event.isLinkToEvent()) {
            //resolve event
            event = get(event.stream, event.version);
        }
        EventRecord linkTo = EventRecord.System.createLinkTo(stream, event);
        return append(linkTo);
    }

    public void emit(String stream, EventRecord event) {
        EventRecord withStream = EventRecord.create(stream, event.type, event.data, event.metadata);
        append(withStream);
    }

    public EventRecord get(String stream, int version) {
        long streamHash = streams.hashOf(stream);

        if (version <= IndexEntry.NO_VERSION) {
            throw new IllegalArgumentException("Version must be greater than " + IndexEntry.NO_VERSION);
        }
        Optional<IndexEntry> indexEntry = index.get(streamHash, version);
        if (!indexEntry.isPresent()) {
            //TODO improve this to a non exception response
            throw new RuntimeException("IndexEntry not found for " + stream + "@" + version);
        }

        return indexEntry.map(this::get).orElseThrow(() -> new RuntimeException("EventRecord not found for " + indexEntry));

    }


    //TODO make it price and change SingleStreamIterator
    EventRecord get(IndexEntry indexEntry) {
        Objects.requireNonNull(indexEntry, "IndexEntry must be provided");
        EventRecord record = eventLog.get(indexEntry.position);

        return resolve(record);
    }

    private EventRecord resolve(EventRecord record) {
        if (record.isLinkToEvent()) {
            String[] split = record.dataAsString().split(EventRecord.STREAM_VERSION_SEPARATOR);
            var linkToStream = split[0];
            var linkToVersion = Integer.parseInt(split[1]);
            return get(linkToStream, linkToVersion);
        }
        return record;
    }

    public EventRecord append(EventRecord event) {
        return append(event, IndexEntry.NO_VERSION);
    }

    public EventRecord append(EventRecord event, int expectedVersion) {
        Objects.requireNonNull(event, "Event must be provided");
        StringUtils.requireNonBlank(event.stream, "stream must be provided");

        StreamMetadata metadata = getOrCreateStream(event.stream);
        return append(metadata, event, expectedVersion);
    }

    private StreamMetadata getOrCreateStream(String stream) {
        long streamHash = streams.hashOf(stream);
        return streams.get(streamHash).orElseGet(() -> {
            StreamMetadata streamMetadata = new StreamMetadata(stream, streamHash, System.currentTimeMillis());
            if (streams.create(streamMetadata)) {
                EventRecord eventRecord = EventRecord.System.streamCreatedRecord(streamMetadata);
                this.append(eventRecord);
            }
            return streamMetadata;
        });
    }

    private EventRecord append(StreamMetadata streamMetadata, EventRecord event, int expectedVersion) {
        if (streamMetadata == null) {
            throw new IllegalArgumentException("EventStream cannot be null");
        }
        long streamHash = streamMetadata.hash;
        int version = streams.tryIncrementVersion(streamHash, expectedVersion);

        var record = new EventRecord(event.stream, event.type, version, System.currentTimeMillis(), event.data, event.metadata);

        long position = eventLog.append(record);
        index.add(streamHash, version, position);

        return record;
    }

    public PollingSubscriber<EventRecord> poller() {
        return new LogPoller(eventLog.poller(), this);
    }

    public PollingSubscriber<EventRecord> poller(long position) {
        return new LogPoller(eventLog.poller(position), this);
    }

    public PollingSubscriber<EventRecord> poller(String stream) {
        Set<Long> hashes = streams.streamMatching(stream).stream().map(streams::hashOf).collect(Collectors.toSet());
        return new IndexedLogPoller(index.poller(hashes), this);
    }

    public PollingSubscriber<EventRecord> poller(Set<String> streamNames) {
        Set<Long> hashes = streamNames.stream().map(streams::hashOf).collect(Collectors.toSet());
        return new IndexedLogPoller(index.poller(hashes), this);
    }

//    public PollingSubscriber<Event> poller(String stream, int version) {
//        long streamHash = streams.hashOf(stream);
//        return new IndexedLogPoller(index.poller(streamHash, version), eventLog);
//    }

    private LogIterator<IndexEntry> withMaxCountFilter(long streamHash, LogIterator<IndexEntry> iterator) {
        return streams.get(streamHash)
                .map(stream -> stream.maxCount)
                .filter(maxCount -> maxCount > 0)
                .map(maxCount -> MaxCountFilteringIterator.of(maxCount, streams.version(streamHash), iterator))
                .orElse(iterator);
    }

    private LogIterator<EventRecord> withMaxAgeFilter(Set<Long> streamHashes, LogIterator<EventRecord> iterator) {
        Map<String, StreamMetadata> metadataMap = streamHashes.stream()
                .map(streams::get)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toMap(StreamMetadata::name, meta -> meta));

        return new MaxAgeFilteringIterator(metadataMap, iterator);
    }

    @Override
    public void close() {
        index.close();
        eventLog.close();
        streams.close();
    }

    private static class LogPoller implements PollingSubscriber<EventRecord> {

        private final EventStore store;
        private final PollingSubscriber<EventRecord> logPoller;

        private LogPoller(PollingSubscriber<EventRecord> logPoller, EventStore store) {
            this.store = store;
            this.logPoller = logPoller;
        }

        @Override
        public EventRecord peek() throws InterruptedException {
            return store.resolve(logPoller.peek());
        }

        @Override
        public EventRecord poll() throws InterruptedException {
            return store.resolve(logPoller.poll());
        }

        @Override
        public EventRecord poll(long limit, TimeUnit timeUnit) throws InterruptedException {
            return store.resolve(logPoller.poll(limit, timeUnit));
        }

        @Override
        public EventRecord take() throws InterruptedException {
            return store.resolve(logPoller.take());
        }

        @Override
        public boolean headOfLog() {
            return logPoller.headOfLog();
        }

        @Override
        public boolean endOfLog() {
            return false;
        }

        @Override
        public long position() {
            return logPoller.position();
        }

        @Override
        public void close() throws IOException {
            logPoller.close();
        }
    }

    private static class IndexedLogPoller implements PollingSubscriber<EventRecord> {

        private final EventStore store;
        private final PollingSubscriber<IndexEntry> indexPoller;

        private IndexedLogPoller(PollingSubscriber<IndexEntry> indexPoller, EventStore store) {
            this.indexPoller = indexPoller;
            this.store = store;
        }

        private EventRecord getOrElse(IndexEntry peek) {
            return Optional.ofNullable(peek).map(store::get).orElse(null);
        }

        @Override
        public EventRecord peek() throws InterruptedException {
            IndexEntry peek = indexPoller.peek();
            return getOrElse(peek);
        }

        @Override
        public EventRecord poll() throws InterruptedException {
            IndexEntry poll = indexPoller.poll();
            return getOrElse(poll);
        }

        @Override
        public EventRecord poll(long limit, TimeUnit timeUnit) throws InterruptedException {
            IndexEntry poll = indexPoller.poll(limit, timeUnit);
            return getOrElse(poll);
        }

        @Override
        public EventRecord take() throws InterruptedException {
            IndexEntry take = indexPoller.take();
            return getOrElse(take);
        }

        @Override
        public boolean headOfLog() {
            return indexPoller.headOfLog();
        }

        @Override
        public boolean endOfLog() {
            return indexPoller.endOfLog();
        }

        @Override
        public long position() {
            return -1; //TODO this is not reliable for index log
        }

        @Override
        public void close() throws IOException {
            indexPoller.close();
        }
    }
}