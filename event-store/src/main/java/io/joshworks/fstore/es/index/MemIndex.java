package io.joshworks.fstore.es.index;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

public class MemIndex implements Index {

    final AtomicInteger size = new AtomicInteger();
    final SortedSet<IndexEntry> index = new TreeSet<>();

    void add(long stream, int version, long position) {
        index.add(IndexEntry.of(stream, version, position));
        size.incrementAndGet();
    }

    @Override
    public List<IndexEntry> range(Range range) {
        return new ArrayList<>(index.subSet(range.start(), range.end()));
    }

    @Override
    public Optional<IndexEntry> latestOfStream(long stream) {
        List<IndexEntry> all = range(Range.allOf(stream));
        return all.isEmpty() ? Optional.empty() : Optional.of(all.get(all.size() - 1));
    }

    public int size() {
        return size.get();
    }


    @Override
    public void close() {
        size.set(0);
        index.clear();
    }

    @Override
    public Iterator<IndexEntry> iterator(Range range) {
        return new ArrayList<>(index.subSet(range.start(), range.end())).iterator();
    }

    @Override
    public Optional<IndexEntry> get(long stream, int version) {
        Range range = Range.of(stream, version, version + 1);
        SortedSet<IndexEntry> entries = index.subSet(range.start(), range.end());
        if(entries.isEmpty()) {
            return Optional.empty();
        }
        if(entries.size() > 1) {
            //TODO improve message
            throw new IllegalStateException("Found more than one index entry");
        }
        return Optional.of(entries.first());
    }

    @Override
    public Iterator<IndexEntry> iterator() {
        return new ArrayList<>(index).iterator();
    }
}
