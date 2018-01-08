package serializer;

import io.joshworks.fstore.event.Event;

public interface EventSerializer {
    byte[] toBytes(Event event);

    Event fromBytes(byte[] data);
}
