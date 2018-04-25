package io.joshworks.fstore.es.index;

import java.io.Closeable;
import java.util.Iterator;
import java.util.Optional;

public interface Index extends Searchable, Closeable, Iterable<IndexEntry> {

    Iterator<IndexEntry> iterator(Range range);

    Optional<IndexEntry> get(long stream, int version);

}
