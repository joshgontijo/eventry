package io.joshworks.fstore.es;

import java.util.UUID;

public class Event {

    public final String uuid;
    public final String type;
    public final String data;
    public final long timestamp;

    public Event(String uuid, String type, String data, long timestamp) {
        this.uuid = uuid;
        this.type = type;
        this.data = data;
        this.timestamp = timestamp;
    }

    public static Event create(String type, String data) {
        return new Event(UUID.randomUUID().toString(), type, data, System.currentTimeMillis());
    }

    @Override
    public String toString() {
        return "Event{" + "type='" + type + '\'' +
                ", data='" + data + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
