package io.joshworks.fstore.es;

import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.es.hash.Murmur3Hash;
import io.joshworks.fstore.es.hash.XXHash;
import io.joshworks.fstore.es.index.IndexEntry;
import io.joshworks.fstore.es.index.Range;
import io.joshworks.fstore.es.index.StreamHasher;
import io.joshworks.fstore.es.index.TableIndex;
import io.joshworks.fstore.es.log.Event;
import io.joshworks.fstore.es.log.EventLog;
import io.joshworks.fstore.es.log.EventSerializer;
import io.joshworks.fstore.es.stream.StreamInfo;
import io.joshworks.fstore.es.stream.StreamMetadata;
import io.joshworks.fstore.es.stream.Streams;
import io.joshworks.fstore.es.utils.Tuple;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.LogAppender;

import java.io.Closeable;
import java.io.File;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class EventStore implements Closeable {

    //TODO expose
    private static final int LRU_CACHE_SIZE = 1000000;

    private final TableIndex index;
    private final StreamHasher hasher;
    private final Streams streams;
    private final EventLog eventLog;

    private EventStore(File rootDir) {
        this.eventLog = new EventLog(LogAppender.builder(rootDir, new EventSerializer()).segmentSize(209715200).disableCompaction());
        this.index = new TableIndex(rootDir);
        this.hasher = new StreamHasher(new XXHash(), new Murmur3Hash());
        this.streams = new Streams(rootDir, LRU_CACHE_SIZE, this.index::version);
    }

    public static EventStore open(File rootDir) {
        return new EventStore(rootDir);
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

    public void createStream(String name, int maxCount, long maxAge, Map<String, Integer> permissions, Map<String, String> metadata) {
        long hash = hasher.hash(name);
        streams.add(new StreamMetadata(name, hash, System.currentTimeMillis(), maxAge, maxCount, permissions, metadata));
    }

    public List<StreamInfo> streamsMetadata() {
        return streams.all().stream().map(meta -> {
            int version = streams.version(meta.hash);
            return StreamInfo.from(meta, version);
        }).collect(Collectors.toList());
    }

    public Optional<StreamInfo> streamMetadata(String stream) {
        long streamHash = hasher.hash(stream);
        return streams.get(streamHash).map(meta -> {
            int version = streams.version(meta.hash);
            return StreamInfo.from(meta, version);
        });
    }

    public LogIterator<Event> fromStreamIter(String stream) {
        return fromStreamIter(stream, Range.START_VERSION);
    }

    public Stream<Event> fromStream(String stream) {
        return fromStream(stream, Range.START_VERSION);
    }

    public LogIterator<Event> fromStreamIter(String stream, int versionInclusive) {
        long streamHash = hasher.hash(stream);
        LogIterator<IndexEntry> addresses = index.iterator(Range.of(streamHash, versionInclusive));
        addresses = withMaxCountFilter(streamHash, addresses);
        return withMaxAgeFilter(Set.of(streamHash), new SingleStreamIterator(stream, addresses, eventLog));
    }

    public Stream<Event> fromStream(String stream, int versionInclusive) {
        LogIterator<Event> iterator = fromStreamIter(stream, versionInclusive);
        return Iterators.stream(iterator);
    }

    public Stream<Event> zipStreams(Set<String> streams) {
        LogIterator<Event> iterator = zipStreamsIter(streams);
        return Iterators.stream(iterator);
    }

    public LogIterator<Event> zipStreamsIter(String streamPrefix) {
        Set<String> eventStreams = streams.streamsStartingWith(streamPrefix);
        return zipStreamsIter(eventStreams);
    }

    public Stream<Event> zipStreams(String streamPrefix) {
        return Iterators.stream(zipStreamsIter(streamPrefix));
    }

    public LogIterator<Event> zipStreamsIter(Set<String> streams) {
        Set<String> uniqueStreams = new LinkedHashSet<>(streams);

        List<LogIterator<IndexEntry>> indexes = new ArrayList<>(streams.size());
        Map<Long, String> mappings = new HashMap<>();
        Set<Long> hashes = new HashSet<>();
        for (String stream : uniqueStreams) {
            if (stream == null || stream.isEmpty()) {
                throw new IllegalArgumentException("Stream cannot empty");
            }
            long streamHash = hasher.hash(stream);
            hashes.add(streamHash);
            LogIterator<IndexEntry> indexStream = index.iterator(Range.allOf(streamHash));

            indexes.add(indexStream);
            mappings.put(streamHash, stream);
        }

        return withMaxAgeFilter(hashes, new MultiStreamIterator(mappings, indexes, eventLog));
    }

    public Stream<Stream<Event>> fromStreams(Set<String> streams) {
        return streams.stream().map(this::fromStream);
    }

    public Map<String, Stream<Event>> fromStreamsMapped(Set<String> streams) {
        return streams.stream()
                .map(stream -> Tuple.of(stream, fromStream(stream)))
                .collect(Collectors.toMap(Tuple::a, Tuple::b));
    }

    public int version(String stream) {
        long streamHash = hasher.hash(stream);
        return streams.version(streamHash);
    }

    public LogIterator<Event> fromAllIter() {
        return eventLog.scanner();
    }

    //Won't return the stream in the event !
    public Stream<Event> fromAll() {
        return eventLog.stream();
    }

    public void linkTo(String stream, Event event) {
        long streamHash = hasher.hash(stream);

        int newVersion = streams.tryIncrementVersion(streamHash, IndexEntry.NO_VERSION);
        index.add(streamHash, newVersion, event.position());
    }

    public void poller() {

    }

    public Optional<Event> get(String stream, int version) {
        if (version <= 0) {
            throw new IllegalArgumentException("Version must be greater than zero");
        }
        long streamHash = hasher.hash(stream);
        Range range = Range.of(streamHash, version, version + 1);
        return index.stream(range).map(i -> {
            Event event = eventLog.get(i.position);
            event.streamInfo(stream, i);
            return event;
        }).findFirst();
    }

    public Event add(Event event) {
        return add(event, IndexEntry.NO_VERSION);
    }

    public Event add(Event event, int expectedVersion) {
        Objects.requireNonNull(event, "Event must be provided");
        if (event.stream() == null || event.stream().isEmpty()) {
            throw new IllegalArgumentException("Invalid stream");
        }
        String stream = event.stream();
        long streamHash = hasher.hash(stream);
        StreamMetadata streamMetadata = streams.get(streamHash).orElseGet(() -> createStream(stream, streamHash));

        return add(streamMetadata, event, expectedVersion);
    }

    private Event add(StreamMetadata streamMetadata, Event event, int expectedVersion) {
        if(streamMetadata == null) {
            throw new IllegalArgumentException("EventStream cannot be null");
        }
        long streamHash = streamMetadata.hash;

        int version = streams.tryIncrementVersion(streamHash, expectedVersion);

        event.stream(streamMetadata.name);
        long position = eventLog.append(event);
        IndexEntry indexEntry = index.add(streamHash, version, position);

        event.streamInfo(streamMetadata.name, indexEntry);
        return event;
    }

    private StreamMetadata createStream(String name, long hash) {
        StreamMetadata streamMetadata = new StreamMetadata(name, hash, System.currentTimeMillis());
        streams.add(streamMetadata);
        return streamMetadata;
    }

    private LogIterator<IndexEntry> withMaxCountFilter(long streamHash, LogIterator<IndexEntry> iterator) {
        return streams.get(streamHash)
                .map(stream -> stream.maxCount)
                .filter(maxCount -> maxCount > 0)
                .map(maxCount -> MaxCountFilteringIterator.of(maxCount, streams.version(streamHash), iterator))
                .orElse(iterator);
    }

    private LogIterator<Event> withMaxAgeFilter(Set<Long> streamHashes, LogIterator<Event> iterator) {
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
}