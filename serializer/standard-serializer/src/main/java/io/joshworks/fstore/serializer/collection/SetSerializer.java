package io.joshworks.fstore.serializer.collection;

import io.joshworks.fstore.core.Serializer;

import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

public class SetSerializer<V> extends CollectionSerializer<V, Set<V>> {

    public SetSerializer(Serializer<V> valueSerializer, Function<V, Integer> sizeOfValue, Supplier<Set<V>> instanceSupplier) {
        super(valueSerializer, sizeOfValue, instanceSupplier);
    }
}
