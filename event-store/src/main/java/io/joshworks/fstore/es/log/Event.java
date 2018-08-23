package io.joshworks.fstore.es.log;

import io.joshworks.fstore.es.index.IndexEntry;

import java.nio.charset.StandardCharsets;

public class Event {

    private final String type;
    private final byte[] data;
    private final long timestamp;
    //from index
    private String stream;
    private int version = -1;
    private long position = -1;

    private Event(String stream, String type, byte[] data, long timestamp) {
        this.type = type;
        this.data = data;
        this.stream = stream;
        this.timestamp = timestamp;
    }

    public static Event of(String stream, String type, byte[] data, long timestamp) {
        return new Event(stream, type, data, timestamp);
    }

    public static Event create(String stream, String type, String data) {
        return create(stream, type, data == null ? new byte[0] : data.getBytes(StandardCharsets.UTF_8));
    }

    public static Event create(String stream, String type, byte[] data) {
        return new Event(stream, type , data, System.currentTimeMillis());
    }

    public void stream(String stream) {
        this.stream = stream;
    }

    public void version(int version) {
        this.version = version;
    }

    public void streamInfo(String stream, IndexEntry key) {
        stream(stream);
        version(key.version);
        position(key.position);
    }

    public void position(long position) {
        this.position = position;
    }

    public String stream() {
        return stream;
    }

    public int version() {
        return version;
    }

    public String type() {
        return type;
    }

    public byte[] data() {
        return data;
    }

    public long timestamp() {
        return timestamp;
    }

    public long position() {
        return position;
    }

    @Override
    public String toString() {
        return "Event{" +
                "type='" + type + '\'' +
                ", timestamp=" + timestamp +
                ", stream='" + stream + '\'' +
                ", version=" + version +
                ", position=" + position +
                '}';
    }
}
