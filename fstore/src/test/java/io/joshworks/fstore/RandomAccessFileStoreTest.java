package io.joshworks.fstore;

import io.joshworks.fstore.serializer.KryoEventSerializer;
import io.joshworks.fstore.store.RandomAccessDataStore;

public class RandomAccessFileStoreTest extends EventStoreTestBase {

    @Override
    protected EventStore createStore(String name) {
        return new EventStore(new RandomAccessDataStore(name), new KryoEventSerializer());
    }
}
