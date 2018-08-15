package io.joshworks.fstore.server;

import io.joshworks.fstore.es.EventStore;
import io.joshworks.snappy.http.HttpExchange;

public class StreamEndpoint {

    private final EventStore store;

    public StreamEndpoint(EventStore store) {
        this.store = store;
    }

    public void fromStream(HttpExchange exchange) {

    }

    public void create(HttpExchange exchange) {

    }

    public void append(HttpExchange exchange) {

    }

    public void delete(HttpExchange exchange) {

    }

    public void list(HttpExchange exchange) {

    }

    public void metadata(HttpExchange exchange) {

    }
}
