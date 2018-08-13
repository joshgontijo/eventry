package io.joshworks.fstore.serializer.collection;

import io.joshworks.fstore.core.Serializer;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public class ListSerializer<V> extends CollectionSerializer<V, List<V>> {

    public ListSerializer(Serializer<V> valueSerializer, Function<V, Integer> sizeOfValue, Supplier<List<V>> instanceSupplier) {
        super(valueSerializer, sizeOfValue, instanceSupplier);
    }
}
