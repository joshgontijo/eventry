package io.joshworks.fstore.log.appender.history;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.DataReader;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.segment.LogSegment;
import io.joshworks.fstore.log.segment.Type;

public class HistorySegment extends LogSegment<HistoryItem> {

    public HistorySegment(Storage storage, Serializer<HistoryItem> serializer, DataReader reader) {
        super(storage, serializer, reader);
    }

    public HistorySegment(Storage storage, Serializer<HistoryItem> serializer, DataReader reader, Type type) {
        super(storage, serializer, reader, type);
    }
}
