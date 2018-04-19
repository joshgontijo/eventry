package io.joshworks.fstore.es.index;

import java.io.Closeable;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

public class MemIndex implements Searchable, Closeable {

    final AtomicInteger size = new AtomicInteger();
    final SortedSet<IndexEntry> index = new ConcurrentSkipListSet<>();

    void add(long stream, int version, long position) {
        index.add(IndexEntry.of(stream, version, position));
        size.incrementAndGet();
    }

    @Override
    public SortedSet<IndexEntry> range(Range range) {
        return index.subSet(range.start(), range.end());
    }

    @Override
    public Optional<IndexEntry> lastOfStream(long stream) {
        SortedSet<IndexEntry> all = range(Range.allOf(stream));
        return all.isEmpty() ? Optional.empty() : Optional.of(all.last());
    }

    public int size() {
        return size.get();
    }


    @Override
    public void close() {
        size.set(0);
        index.clear();
    }
}
