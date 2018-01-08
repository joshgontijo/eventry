package io.joshworks.fstore.serializer.json;

import com.google.gson.Gson;
import io.joshworks.fstore.api.Event;
import io.joshworks.fstore.api.EventSerializer;


public class JsonSerializer implements EventSerializer {

    private static final Gson gson = new Gson();

    @Override
    public byte[] toBytes(Event event) {
        return gson.toJson(event).getBytes();
    }

    @Override
    public Event fromBytes(byte[] data) {
        return gson.fromJson(new String(data), Event.class);
    }
}
