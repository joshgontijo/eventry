package io.joshworks.fstore.es.index;

import java.util.List;
import java.util.Optional;

public interface Searchable {

    List<IndexEntry> range(Range range);

    Optional<IndexEntry> latestOfStream(long stream);

}
