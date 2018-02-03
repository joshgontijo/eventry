package io.joshworks.fstore.serializer.json;

import com.google.gson.Gson;
import io.joshworks.fstore.api.Event;
import io.joshworks.fstore.api.Serializer;


public class JsonSerializer implements Serializer<String> {

    private static final Gson gson = new Gson();

    @Override
    public byte[] toBytes(Event event) {
        return gson.toJson(event).getBytes();
    }

    @Override
    public Event fromBytes(byte[] data) {
        String stringData = null;
        try {
            stringData = new String(data);
            return gson.fromJson(stringData, Event.class);

        } catch (Exception e) {
            System.err.println("VALUE:" + stringData);
            throw new RuntimeException(e);
        }
    }
}
