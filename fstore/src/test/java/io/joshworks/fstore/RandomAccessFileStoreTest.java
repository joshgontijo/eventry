package io.joshworks.fstore;

import serializer.KryoEventSerializer;
import io.joshworks.fstore.store.RandomAccessDataStore;

public class RandomAccessFileStoreTest extends EventStoreTestBase {

    @Override
    protected EventStore createStore(String name) {
        return new EventStore(new RandomAccessDataStore(name), new KryoEventSerializer());
    }
}
