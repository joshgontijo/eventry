package io.joshworks.fstore.es.projections.script;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.es.log.Event;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class JsonEvent {

    private static final Gson gson = new Gson();
    private static final Serializer<Map<String, Object>> jsonSerializer = JsonSerializer.of(new TypeToken<Map<String, Object>>(){}.getType());
    public String type;
    public Map<String, Object> data;

    //response only
    public long timestamp;
    public String stream;
    public int version = -1;
    public long position = -1;


    public static JsonEvent from(Event event) {
        JsonEvent body = new JsonEvent();
        body.type = event.type();
        body.timestamp = event.timestamp();
        body.version = event.version();
        body.stream = event.stream();
        body.position = event.position();
        body.data = jsonSerializer.fromBytes(ByteBuffer.wrap(event.data()));

        return body;
    }

    public Event toEvent() {
        Event event = Event.of(stream, type, jsonSerializer.toBytes(data).array(), System.currentTimeMillis());
        event.position(position);
        event.version(version);
        return event;
    }

    public String toJson() {
        return gson.toJson(this);
    }

    @Override
    public String toString() {
        return toJson();
    }
}
