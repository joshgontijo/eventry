package io.joshworks.fstore.api;

public interface EventSerializer {
    byte[] toBytes(Event event);

    Event fromBytes(byte[] data);
}
