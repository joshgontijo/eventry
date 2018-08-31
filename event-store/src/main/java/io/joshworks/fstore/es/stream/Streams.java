package io.joshworks.fstore.es.stream;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.es.LRUCache;
import io.joshworks.fstore.es.index.IndexEntry;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.log.appender.appenders.SimpleLogAppender;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Streams implements Closeable {

    //TODO LRU cache ? there's no way of getting item by stream name, need to use an indexed lsm-tree
    //LRU map that reads the last version from the index
    private final LRUCache<Long, AtomicInteger> versions;
    private final Map<Long, StreamMetadata> streamsMap;
    private final SimpleLogAppender<StreamMetadata> appender;

    private static final String DIRECTORY = "streams";

    public Streams(File root, int versionLruCacheSize, Function<Long, Integer> versionFetcher) {
        this.appender = new SimpleLogAppender<>(LogAppender.builder(new File(root, DIRECTORY), new EventStreamSerializer()));
        this.streamsMap = loadFromDisk(this.appender);
        this.versions = new LRUCache<>(versionLruCacheSize, streamHash -> new AtomicInteger(versionFetcher.apply(streamHash)));
    }

    public Optional<StreamMetadata> get(long streamHash) {
        return Optional.ofNullable(streamsMap.get(streamHash));
    }

    public List<StreamMetadata> all() {
        return new ArrayList<>(streamsMap.values());
    }

    //TODO implement 'remove'
    //TODO field validation needed
    public void add(StreamMetadata stream) {
        Objects.requireNonNull(stream);
        appender.append(stream);
        streamsMap.put(stream.hash, stream);
    }

    //Only supports 'startingWith' wildcard
    //EX: users-*
    public Set<String> streamMatching(String value) {
        if(value == null) {
            return new HashSet<>();
        }
        //wildcard
        if(value.endsWith("*")) {
            final String prefix = value.substring(0, value.length() - 1);
            return streamsMap.values().stream()
                    .filter(stream -> stream.name.startsWith(prefix))
                    .map(stream -> stream.name)
                    .collect(Collectors.toSet());
        }

        return streamsMap.values().stream()
                .filter(stream -> stream.name.equals(value))
                .map(stream -> stream.name)
                .collect(Collectors.toSet());


    }

    public int version(long stream) {
        return versions.getOrElse(stream, new AtomicInteger(IndexEntry.NO_VERSION)).get();
    }

    public int tryIncrementVersion(long stream, int expected) {
        AtomicInteger versionCounter = versions.getOrElse(stream, new AtomicInteger(IndexEntry.NO_VERSION));
        if(expected < 0) {
            return versionCounter.incrementAndGet();
        }
        int newValue = expected + 1;
        if(!versionCounter.compareAndSet(expected, newValue)) {
            throw new IllegalArgumentException("Version mismatch: expected stream " + stream + " version is higher than expected: " + expected);
        }
        return newValue;
    }


    private static Map<Long, StreamMetadata> loadFromDisk(SimpleLogAppender<StreamMetadata> appender) {
        Map<Long, StreamMetadata> map = new ConcurrentHashMap<>();
        try (LogIterator<StreamMetadata> scanner = appender.scanner()) {

            while (scanner.hasNext()) {
                StreamMetadata next = scanner.next();
                map.put(next.hash, next);
            }

        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }

        return map;
    }

    @Override
    public void close() {
        appender.close();
    }

}
