package io.joshworks.fstore.serializer.collection;

import io.joshworks.fstore.core.Serializer;

import java.util.Queue;
import java.util.function.Function;
import java.util.function.Supplier;

public class QueueSerializer<V> extends CollectionSerializer<V, Queue<V>> {

    public QueueSerializer(Serializer<V> valueSerializer, Function<V, Integer> sizeOfValue, Supplier<Queue<V>> instanceSupplier) {
        super(valueSerializer, sizeOfValue, instanceSupplier);
    }
}
