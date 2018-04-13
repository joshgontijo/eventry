package io.joshworks.fstore.es;

public class Event {

    public final String type;
    public final String data;

    public Event(String type, String data) {
        this.type = type;
        this.data = data;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("Event{");
        sb.append("type='").append(type).append('\'');
        sb.append(", data='").append(data).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
