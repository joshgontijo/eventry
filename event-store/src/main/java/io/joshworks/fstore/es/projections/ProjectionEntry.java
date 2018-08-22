package io.joshworks.fstore.es.projections;

import io.joshworks.fstore.es.index.IndexEntry;

public class ProjectionEntry {
    public final int type;
    public final long timestamp;
    public final IndexEntry indexEntry;

    public ProjectionEntry(int type, long timestamp, IndexEntry indexEntry) {
        this.type = type;
        this.timestamp = timestamp;
        this.indexEntry = indexEntry;
    }
}
