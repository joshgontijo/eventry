package io.joshworks.fstore;

import io.joshworks.fstore.serializer.KryoEventSerializer;
import io.joshworks.fstore.store.MappedDataStore;

public class MappedBufferStoreTest extends EventStoreTestBase {

    @Override
    protected EventStore createStore(String name) {
        return new EventStore(new MappedDataStore(name), new KryoEventSerializer());
    }
}
