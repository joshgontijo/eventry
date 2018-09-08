package io.joshworks.fstore.es.data;

import io.joshworks.fstore.es.log.EventRecord;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.nio.ByteBuffer;

import static io.joshworks.fstore.es.data.Constant.SYSTEM_PREFIX;

public class IndexFlushed {

    private static final JsonSerializer<IndexFlushed> serializer = JsonSerializer.of(IndexFlushed.class);

    public final long logPosition;
    public final long timeTaken;
    public final int entries;

    public static final String TYPE = SYSTEM_PREFIX + "INDEX_FLUSHED";

    private IndexFlushed(long logPosition, long timeTaken, int entries) {
        this.logPosition = logPosition;
        this.timeTaken = timeTaken;
        this.entries = entries;
    }

    public static EventRecord create(long logPosition, long timeTaken, int entries) {
        var indexFlushed = new IndexFlushed(logPosition, timeTaken, entries);
        var data = serializer.toBytes(indexFlushed);
        return EventRecord.create(SystemStreams.INDEX, TYPE, data.array());
    }

    public static IndexFlushed from(EventRecord record) {
        return serializer.fromBytes(ByteBuffer.wrap(record.data));
    }

}
