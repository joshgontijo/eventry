package io.joshworks.fstore.es.data;

import io.joshworks.fstore.es.log.EventRecord;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.nio.ByteBuffer;

import static io.joshworks.fstore.es.data.Constant.SYSTEM_PREFIX;

public class StreamDeleted {

    //serializing straight into a StreamMetadata
    private static final JsonSerializer<StreamDeleted> serializer = JsonSerializer.of(StreamDeleted.class);

    public final String stream;
    public final int versionAtDeletion;

    public static final String TYPE = SYSTEM_PREFIX + "STREAM_DELETED";

    public StreamDeleted(String stream, int versionAtDeletion) {
        this.stream = stream;
        this.versionAtDeletion = versionAtDeletion;
    }

    public static EventRecord create(String stream, int versionAtDeletion) {
        var data = serializer.toBytes(new StreamDeleted(stream, versionAtDeletion));
        return EventRecord.create(SystemStreams.STREAMS, TYPE, data.array());
    }

    public static StreamDeleted from(EventRecord record) {
        return serializer.fromBytes(ByteBuffer.wrap(record.data));
    }

}
