package io.joshworks.fstore;

import io.joshworks.fstore.event.Event;
import io.joshworks.fstore.serializer.EventSerializer;
import io.joshworks.fstore.store.DataStore;
//TODO extract superclass once an 'Engine is defined'
public class EventStore {

    private final DataStore store;
    private final EventSerializer serializer;

    public EventStore(DataStore store, EventSerializer serializer) {
        this.store = store;
        this.serializer = serializer;
    }

    public Position save(Event event) {
//        try {
//            byte[] data = serializer.toBytes(event);
//            long position = store.write(data);
//            return Position.of(position, data.length);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
        return null;
    }

    public Event get(Position position) {
//        try {
//            byte[] data = store.read(position.start);
//            return serializer.fromBytes(data);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
        return null;
    }

    public void close()  {
        store.close();
    }


}
