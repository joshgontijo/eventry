package io.joshworks.fstore.es.index;

import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.SortedSet;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.TreeSet;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class MemIndex implements Index {

    final SortedSet<IndexEntry> index = new TreeSet<>();

    void add(long stream, int version, long position) {
        index.add(IndexEntry.of(stream, version, position));
    }

    @Override
    public int version(long stream) {
        Range range = Range.allOf(stream);
        SortedSet<IndexEntry> subset = index.subSet(range.start(), range.end());

        return subset.isEmpty() ? 0 : subset.last().version;
    }

    @Override
    public long size() {
        return index.size();
    }


    @Override
    public void close() {
        index.clear();
    }

    @Override
    public Iterator<IndexEntry> iterator(Range range) {
        return Collections.unmodifiableSet(index.subSet(range.start(), range.end())).iterator();
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
        Range range = Range.of(stream, version, version + 1);
        SortedSet<IndexEntry> entries = index.subSet(range.start(), range.end());
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
        return Collections.unmodifiableSet(index).iterator();
    }
}