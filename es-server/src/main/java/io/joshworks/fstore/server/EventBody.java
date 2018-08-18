package io.joshworks.fstore.server;

import com.google.gson.reflect.TypeToken;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.es.log.Event;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class EventBody {

    private static final Serializer<Map<String, Object>> jsonSerializer = JsonSerializer.of(new TypeToken<Map<String, Object>>(){}.getType());
    public String type;
    public Map<String, Object> data;

    //response only
    public long timestamp;
    public long sequence;
    public String stream;
    public int version = -1;
    public long id = -1;


    public static EventBody from(Event event) {
        EventBody body = new EventBody();
        body.type = event.type();
        body.timestamp = event.timestamp();
        body.version = event.version();
        body.sequence = event.sequence();
        body.stream = event.stream();
        body.id = event.position();
        body.data = jsonSerializer.fromBytes(ByteBuffer.wrap(event.data()));

        return body;
    }

    public Event asEvent() {
        return Event.of(sequence, stream, type, jsonSerializer.toBytes(data).array(), System.currentTimeMillis());
    }


}
