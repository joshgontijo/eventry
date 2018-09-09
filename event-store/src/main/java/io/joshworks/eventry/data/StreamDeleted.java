package io.joshworks.eventry.data;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.nio.ByteBuffer;

public class StreamDeleted {

    //serializing straight into a StreamMetadata
    private static final JsonSerializer<StreamDeleted> serializer = JsonSerializer.of(StreamDeleted.class);

    public final String stream;
    public final int versionAtDeletion;

    public static final String TYPE = Constant.SYSTEM_PREFIX + "STREAM_DELETED";

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
