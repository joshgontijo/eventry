package io.joshworks.fstore.es;

import io.joshworks.fstore.core.util.Iterators;
import io.joshworks.fstore.es.hash.Murmur3Hash;
import io.joshworks.fstore.es.hash.XXHash;
import io.joshworks.fstore.es.index.IndexEntry;
import io.joshworks.fstore.es.index.Range;
import io.joshworks.fstore.es.index.StreamHasher;
import io.joshworks.fstore.es.index.TableIndex;
import io.joshworks.fstore.es.log.Event;
import io.joshworks.fstore.es.log.EventLog;
import io.joshworks.fstore.es.log.EventSerializer;
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

    public Iterator<Event> fromStreamIter(String stream) {
        return fromStreamIter(stream, 1);
    }

    public Stream<Event> fromStream(String stream) {
        return fromStream(stream, 1);
    }

    public Iterator<Event> fromStreamIter(String stream, int versionInclusive) {
        long streamHash = hasher.hash(stream);
        Iterator<IndexEntry> addresses = index.iterator(Range.of(streamHash, versionInclusive));
        addresses = withMaxCountFilter(streamHash, addresses);
        return withMaxAgeFilter(Set.of(streamHash), new SingleStreamIterator(stream, addresses, eventLog));
    }

    public Stream<Event> fromStream(String stream, int versionInclusive) {
        Iterator<Event> iterator = fromStreamIter(stream, versionInclusive);
        return Iterators.stream(iterator);
    }

    public Stream<Event> zipStreams(Set<String> streams) {
        Iterator<Event> iterator = zipStreamsIter(streams);
        return Iterators.stream(iterator);
    }

    public Iterator<Event> zipStreamsIter(String streamPrefix) {
        Set<String> eventStreams = streams.streamsStartingWith(streamPrefix);
        return zipStreamsIter(eventStreams);
    }

    public Stream<Event> zipStreams(String streamPrefix) {
        return Iterators.stream(zipStreamsIter(streamPrefix));
    }

    public Iterator<Event> zipStreamsIter(Set<String> streams) {
        Set<String> uniqueStreams = new LinkedHashSet<>(streams);

        List<Iterator<IndexEntry>> indexes = new ArrayList<>(streams.size());
        Map<Long, String> mappings = new HashMap<>();
        Set<Long> hashes = new HashSet<>();
        for (String stream : uniqueStreams) {
            if (stream == null || stream.isEmpty()) {
                throw new IllegalArgumentException("Stream cannot empty");
            }
            long streamHash = hasher.hash(stream);
            hashes.add(streamHash);
            Iterator<IndexEntry> indexStream = index.iterator(Range.allOf(streamHash));

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

        int newVersion = streams.tryIncrementVersion(streamHash, -1);
        index.add(streamHash, newVersion, event.position());

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

    public void add(String stream, Event event) {
        add(stream, event, -1);
    }

    public void add(String streamName, Event event, int expectedVersion) {
        if (streamName == null || streamName.isEmpty()) {
            throw new IllegalArgumentException("Invalid stream");
        }
        long streamHash = hasher.hash(streamName);
        StreamMetadata stream = streams.get(streamHash).orElseGet(() -> createStream(streamName, streamHash));

        add(stream, event, expectedVersion);
    }

    private void add(StreamMetadata stream, Event event, int expectedVersion) {
        if(stream == null) {
            throw new IllegalArgumentException("EventStream cannot be null");
        }
        long streamHash = stream.hash;

        int version = streams.tryIncrementVersion(streamHash, expectedVersion);

        long position = eventLog.append(event);
        index.add(streamHash, version, position);
    }

    private StreamMetadata createStream(String name, long hash) {
        StreamMetadata streamMetadata = new StreamMetadata(name, hash, System.currentTimeMillis());
        streams.add(streamMetadata);
        return streamMetadata;
    }

    private Iterator<IndexEntry> withMaxCountFilter(long streamHash, Iterator<IndexEntry> iterator) {
        return streams.get(streamHash)
                .map(stream -> stream.maxCount)
                .filter(maxCount -> maxCount > 0)
                .map(maxCount -> MaxCountFilteringIterator.of(maxCount, streams.version(streamHash), iterator))
                .orElse(iterator);
    }

    private Iterator<Event> withMaxAgeFilter(Set<Long> streamHashes, Iterator<Event> iterator) {
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