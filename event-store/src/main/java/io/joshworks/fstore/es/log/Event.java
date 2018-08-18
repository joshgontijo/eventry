package io.joshworks.fstore.es.log;

import io.joshworks.fstore.es.index.IndexEntry;

import java.nio.charset.StandardCharsets;

public class Event {

    private final String type;
    private final byte[] data;
    private final long timestamp;
    private long sequence;
    //from index
    private String stream;
    private int version = -1;
    private long position = -1;

    private Event(long sequence, String type, String stream, byte[] data, long timestamp) {
        this.type = type;
        this.data = data;
        this.stream = stream;
        this.timestamp = timestamp;
        this.sequence = sequence;
    }

    public static Event of(long sequence, String stream, String type, byte[] data, long timestamp) {
        return new Event(sequence, type, stream, data, timestamp);
    }

    public static Event create(String stream, String type, String data) {
        return create(stream, type, data == null ? new byte[0] : data.getBytes(StandardCharsets.UTF_8));
    }

    public static Event create(String stream, String type, byte[] data) {
        return new Event(-1, type, stream, data, System.currentTimeMillis());
    }

    public void stream(String stream) {
        this.stream = stream;
    }

    public void version(int version) {
        this.version = version;
    }

    void sequence(long sequence) {
        this.sequence = sequence;
    }

    public long sequence() {
        return sequence;
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
                ", sequence=" + sequence +
                ", stream='" + stream + '\'' +
                ", version=" + version +
                ", position=" + position +
                '}';
    }
}
