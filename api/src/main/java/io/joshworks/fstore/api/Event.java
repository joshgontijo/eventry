package io.joshworks.fstore.api;


import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Created by Josue on 02/10/2016.
 */
public class Event implements Serializable {

    private static final Map<String, Class<? extends EventType>> eventsTypes = new HashMap<>();
    private static final Map<Class<? extends EventType>, String> eventsNames = new HashMap<>();

    //TODO for testing purposes these fields are not final
    //must find a decent way of storing / retrieving from DB
    private String uuid; //TODO may not be the best choice to set this value
    private String streamId;
    private String type;
    private int version;
    private long timestamp;
    private byte[] data;

    //TODO removed for testing porpuses, uncomment it when needed
//    private EventType parsedEvent;

    private Event(String streamId, Map<String, Object> data, String eventType, int version, long timestamp) {
        if(eventType == null || eventType.isEmpty()) {
            throw new InvalidEvent("Invalid event type");
        }

        if(timestamp < 0) {
            throw new InvalidEvent("Invalid event timestamp: " + timestamp);
        }
        this.type = eventType;
        this.streamId = streamId;
        this.data = null; //FIXME
        this.version = version;
        this.timestamp = timestamp;
    }

    public static Event create(Map<String, Object> data, String eventType) {
        return create(null, data, eventType, -1);
    }

    public static Event create(String streamId, Map<String, Object> data, String eventType) {
        return create(streamId, data, eventType, -1);
    }

    public static Event create(String streamId, Map<String, Object> data, String eventType, int version) {
        return new Event(streamId, data,  eventType, version, System.currentTimeMillis());
    }

    public static <E extends EventType> Event create(String streamId, E data) {
        return create(streamId, data, -1);
    }

    public static <E extends EventType> Event create(String streamId, E data, int version) {
//        String evType = eventsNames.get(data.getClass());
//        Map<String, Object> dataMap = gson.fromJson(gson.toJson(data), new TypeToken<Map<String, Object>>() {
//        }.getType());
//
//        return create(streamId, dataMap, evType, version);
        return null;
    }

    public static Event of(String streamId, Map<String, Object> data, String eventType, int version, long timestamp) {
        return new Event(streamId, data, eventType, version, timestamp);
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getStream() {
        return streamId;
    }

    public String getType() {
        return type;
    }

    public int getVersion() {
        return version;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Map<String, Object> getData() {
        return null;
    }

    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Event event = (Event) o;
        return version == event.version &&
                timestamp == event.timestamp &&
                Objects.equals(streamId, event.streamId) &&
                Objects.equals(type, event.type) &&
                Objects.equals(data, event.data);
    }

    @Override
    public int hashCode() {

        return Objects.hash(streamId, type, version, timestamp, data);
    }

    @Override
    public String toString() {
        return "{" +
                "\"stream\":" + (streamId == null ? "null" : "\"" + streamId + "\"") + ", " +
                "\"name\":" + (type == null ? "null" : "\"" + type + "\"") + ", " +
                "\"version\":\"" + version + "\"" + ", " +
                "\"timestamp\":\"" + timestamp + "\"" + ", " +
                "\"data\":" + (data == null ? "null" : data) +
                "}";
    }

}
