package io.joshworks.fstore.es.log;

import java.util.UUID;

public class Event {

    private final String uuid;
    private final String type;
    private final String data;
    private final long timestamp;
    //from index
    private String stream;
    private int version;

    private Event(String uuid, String type, String data, long timestamp) {
        this.uuid = uuid;
        this.type = type;
        this.data = data;
        this.timestamp = timestamp;
    }

    static Event load(String uuid, String type, String data, long timestamp) {
        return new Event(uuid, type, data, timestamp);
    }

    public static Event create(String type, String data) {
        return new Event(UUID.randomUUID().toString(), type, data, System.currentTimeMillis());
    }

    void stream(String stream) {
        this.stream = stream;
    }

    void version(int version) {
        this.version = version;
    }

    public String stream() {
        return stream;
    }

    public int version() {
        return version;
    }

    public String uuid() {
        return uuid;
    }

    public String type() {
        return type;
    }

    public String data() {
        return data;
    }

    public long timestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "Event{" + "uuid='" + uuid + '\'' +
                ", type='" + type + '\'' +
                ", data='" + data + '\'' +
                ", timestamp=" + timestamp +
                ", stream='" + stream + '\'' +
                ", version=" + version +
                '}';
    }
}
