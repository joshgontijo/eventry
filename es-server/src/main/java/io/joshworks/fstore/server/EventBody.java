package io.joshworks.fstore.server;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.es.log.Event;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class EventBody {

    private static final Gson gson = new Gson();
    private static final Serializer<Map<String, Object>> jsonSerializer = JsonSerializer.of(new TypeToken<Map<String, Object>>(){}.getType());
    public String type;
    public Map<String, Object> data;

    //response only
    public long timestamp;
    public String stream;
    public int version = -1;
    public long id = -1;


    public static EventBody from(Event event) {
        EventBody body = new EventBody();
        body.type = event.type();
        body.timestamp = event.timestamp();
        body.version = event.version();
        body.stream = event.stream();
        body.id = event.position();
        body.data = jsonSerializer.fromBytes(ByteBuffer.wrap(event.data()));

        return body;
    }

    public Event toEvent() {
        return Event.of(stream, type, jsonSerializer.toBytes(data).array(), System.currentTimeMillis());
    }

    public String toJson() {
        return gson.toJson(this);
    }


}
