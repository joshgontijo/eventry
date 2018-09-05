package io.joshworks.fstore.es.index;

import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;

import java.io.Closeable;
import java.util.Optional;
import java.util.stream.Stream;

public interface Index extends Closeable {

    LogIterator<IndexEntry> iterator(Direction direction);

    LogIterator<IndexEntry> iterator(Direction direction, Range range);

    Stream<IndexEntry> stream(Direction direction);

    Stream<IndexEntry> stream(Direction direction, Range range);

    Optional<IndexEntry> get(long stream, int version);

//    void delete(long stream);

    int version(long stream);

}