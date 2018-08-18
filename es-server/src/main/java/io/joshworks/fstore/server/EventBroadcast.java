package io.joshworks.fstore.server;

import io.joshworks.fstore.es.EventStore;

public class EventBroadcast {

    private final EventStore store;

    public EventBroadcast(EventStore store) {
        this.store = store;

    }
}
