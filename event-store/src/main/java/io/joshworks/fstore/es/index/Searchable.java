package io.joshworks.fstore.es.index;

import java.util.Optional;
import java.util.SortedSet;

public interface Searchable {

    SortedSet<IndexEntry> range(Range range);

    Optional<IndexEntry> latestOfStream(long stream);

}
