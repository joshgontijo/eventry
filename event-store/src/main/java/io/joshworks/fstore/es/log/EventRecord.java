package io.joshworks.fstore.es.log;

import io.joshworks.fstore.es.utils.StringUtils;

import java.nio.charset.StandardCharsets;

public class EventRecord {

    public static final String LINKTO_TYPE = "_>";
    public static final String VERSION_SEPARATOR = "@";

    public final String stream;
    public final String type;
    public final int version;
    public final long timestamp;
    public final byte[] data;
    public final byte[] metadata;


    public EventRecord(String stream, String type, int version, long timestamp, byte[] data, byte[] metadata) {
        this.stream = stream;
        this.type = type;
        this.version = version;
        this.timestamp = timestamp;
        this.data = data;
        this.metadata = metadata;
    }

    public static EventRecord create(String stream, String type, String data) {
        return create(stream, type, StringUtils.toUtf8Bytes(data));
    }

    public static EventRecord create(String stream, String type, String data, String metadata) {
        return create(stream, type, StringUtils.toUtf8Bytes(data), StringUtils.toUtf8Bytes(metadata));
    }

    public static EventRecord create(String stream, String type, byte[] data) {
        return create(stream, type, data, new byte[0]);
    }

    public static EventRecord create(String stream, String type, byte[] data, byte[] metadata) {
        return new EventRecord(stream, type, -1, -1, data, metadata);
    }

    public static EventRecord createLinkTo(String stream, int version, long timestamp, EventRecord event) {
        byte[] data = StringUtils.toUtf8Bytes(event.eventId());
        return new EventRecord(stream, LINKTO_TYPE, version, timestamp, data, new byte[0]);
    }

    public String dataAsString() {
        return new String(data, StandardCharsets.UTF_8);
    }

    public String eventId() {
        return stream + VERSION_SEPARATOR + version;
    }


}
