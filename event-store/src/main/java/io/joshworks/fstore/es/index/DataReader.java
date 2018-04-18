package io.joshworks.fstore.es.index;

import java.util.SortedSet;

public interface DataReader {

    SortedSet<IndexEntry> read(long position);

}
