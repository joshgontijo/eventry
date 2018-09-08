package io.joshworks.fstore.es.data;

import io.joshworks.fstore.es.log.EventRecord;
import io.joshworks.fstore.es.stream.StreamMetadata;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.nio.ByteBuffer;

import static io.joshworks.fstore.es.data.Constant.SYSTEM_PREFIX;

public class StreamCreated {

    //serializing straight into a StreamMetadata
    private static final JsonSerializer<StreamMetadata> serializer = JsonSerializer.of(StreamMetadata.class);

    public static final String TYPE = SYSTEM_PREFIX + "STREAM_CREATED";

    public static EventRecord create(StreamMetadata metadata) {
        var data = serializer.toBytes(metadata);
        return EventRecord.create(SystemStreams.STREAMS, TYPE, data.array());
    }

    public static StreamMetadata from(EventRecord record) {
        return serializer.fromBytes(ByteBuffer.wrap(record.data));
    }

}
