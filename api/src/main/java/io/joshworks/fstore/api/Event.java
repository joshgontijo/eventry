package io.joshworks.fstore.api;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Created by Josue on 02/10/2016.
 */
public class Event implements Serializable {

    private static final Gson gson = new Gson();
    private static final Map<String, Class<? extends EventType>> eventsTypes = new HashMap<>();
    private static final Map<Class<? extends EventType>, String> eventsNames = new HashMap<>();

    private final String streamId;
    private final String type;
    private final int version;
    private final long timestamp;
    private final Map<String, Object> data;
    private EventType parsedEvent;

    private Event(String streamId, Map<String, Object> data, String eventType, int version, long timestamp) {
         if (data == null) {
            throw new InvalidEvent("Invalid event data");
        }
        if (streamId == null || String.valueOf(streamId).isEmpty()) {
            throw new InvalidEvent("Invalid event streams");
        }
        if(eventType == null || eventType.isEmpty()) {
            throw new InvalidEvent("Invalid event type");
        }

        if(timestamp < 0) {
            throw new InvalidEvent("Invalid event timestamp: " + timestamp);
        }
        this.type = eventType;
        this.streamId = streamId;
        this.data = data;
        this.version = version;
        this.timestamp = timestamp;
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
        String evType = eventsNames.get(data.getClass());
        Map<String, Object> dataMap = gson.fromJson(gson.toJson(data), new TypeToken<Map<String, Object>>() {
        }.getType());

        return create(streamId, dataMap, evType, version);
    }

    public static Event of(String streamId, Map<String, Object> data, String eventType, int version, long timestamp) {
        return new Event(streamId, data,  eventType, version, timestamp);
    }

    public static void registerEventMapper(String name, Class<? extends EventType> type) {
        eventsNames.put(type, name);
        eventsTypes.put(name, type);
    }

    public <E extends EventType> E getEvent() {
        if (parsedEvent != null) {
            return (E) parsedEvent;
        }
        Class<E> aClass = (Class<E>) eventsTypes.get(type);
        if (aClass == null) {
            throw new IllegalStateException("No registered event types for name '" + type + "'");
        }
        E event = gson.fromJson(gson.toJson(data), aClass);
        parsedEvent = event;
        return event;
    }

    public <E extends EventType> E dataAs(Class<E> eventType) {
        return gson.fromJson(gson.toJson(data), eventType);
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
        return data;
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
                Objects.equals(data, event.data) &&
                Objects.equals(parsedEvent, event.parsedEvent);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamId, type, version, timestamp, data, parsedEvent);
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
