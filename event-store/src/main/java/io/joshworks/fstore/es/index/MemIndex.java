package io.joshworks.fstore.es.index;

import io.joshworks.fstore.es.utils.Iterators;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class MemIndex implements Index {

    private final Map<Long, SortedSet<IndexEntry>> index = new ConcurrentHashMap<>();
    private final AtomicInteger size = new AtomicInteger();

    void add(long stream, int version, long position) {
        index.compute(stream, (k, v) -> {
            if (v == null)
                v = new TreeSet<>();
            v.add(IndexEntry.of(stream, version, position));
            size.incrementAndGet();
            return v;
        });
    }

    @Override
    public int version(long stream) {
        SortedSet<IndexEntry> entries = index.get(stream);
        if(entries == null) {
            return 0;
        }

        return entries.last().version;
    }

    @Override
    public int size() {
        return size.get();
    }


    @Override
    public void close() {
        index.clear();
        size.set(0);
    }

    @Override
    public Iterator<IndexEntry> iterator(Range range) {
        SortedSet<IndexEntry> entries = index.get(range.stream);
        if(entries == null) {
            return Iterators.empty();
        }
        return Collections.unmodifiableSet(entries.subSet(range.start(), range.end())).iterator();
    }

    @Override
    public Stream<IndexEntry> stream() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator(), Spliterator.ORDERED), false);
    }

    @Override
    public Stream<IndexEntry> stream(Range range) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator(range), Spliterator.ORDERED), false);
    }

    @Override
    public Optional<IndexEntry> get(long stream, int version) {
        SortedSet<IndexEntry> entries = index.get(stream);
        if(entries == null) {
            return Optional.empty();
        }

        Range range = Range.of(stream, version, version + 1);
        entries = entries.subSet(range.start(), range.end());
        if (entries.isEmpty()) {
            return Optional.empty();
        }
        if (entries.size() > 1) {
            //TODO improve message
            throw new IllegalStateException("Found more than one index entry");
        }
        return Optional.of(entries.first());
    }

    @Override
    public Iterator<IndexEntry> iterator() {
        SortedSet<IndexEntry> reduced = index.values().stream().reduce(new TreeSet<>(), (state, next) -> {
            state.addAll(next);
            return state;
        });

        return Collections.unmodifiableSet(reduced).iterator();
    }
}