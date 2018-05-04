package io.joshworks.fstore.es.index;

import java.io.Closeable;
import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Stream;

public interface Index extends Closeable, Iterable<IndexEntry> {

    Iterator<IndexEntry> iterator(Range range);

    Stream<IndexEntry> stream(Range range);

    Optional<IndexEntry> get(long stream, int version);

    int version(long stream);

}