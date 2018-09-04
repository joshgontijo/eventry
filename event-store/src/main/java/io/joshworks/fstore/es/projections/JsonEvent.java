package io.joshworks.fstore.es.projections;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.es.log.EventRecord;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class JsonEvent {

    private static final Gson gson = new Gson();
    private static final Serializer<Map<String, Object>> jsonSerializer = JsonSerializer.of(new TypeToken<Map<String, Object>>(){}.getType());

    public final String type;
    public final Map<String, Object> data;
    public final Map<String, Object> metadata;

    public final long timestamp;
    public final String stream;
    public final int version;


    private JsonEvent(EventRecord event) {
        this.type = event.type;
        this.timestamp = event.timestamp;
        this.version = event.version;
        this.stream = event.stream;
        this.data = jsonSerializer.fromBytes(ByteBuffer.wrap(event.data));
        this.metadata = jsonSerializer.fromBytes(ByteBuffer.wrap(event.metadata));
    }

    public static JsonEvent from(EventRecord event) {
        return new JsonEvent(event);
    }

    public EventRecord toEvent() {
        return new EventRecord(stream, type, version, timestamp, jsonSerializer.toBytes(data).array(), jsonSerializer.toBytes(metadata).array());
    }

    public String toJson() {
        return gson.toJson(this);
    }

    @Override
    public String toString() {
        return toJson();
    }
}
