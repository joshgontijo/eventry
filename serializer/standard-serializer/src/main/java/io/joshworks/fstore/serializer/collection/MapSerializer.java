package io.joshworks.fstore.serializer.collection;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public class MapSerializer<K, V> implements Serializer<Map<K, V>> {

    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private final Supplier<Map<K, V>> instanceSupplier;
    private final Function<K, Integer> sizeOfKey;
    private final Function<V, Integer> sizeOfValue;

    public MapSerializer(Serializer<K> keySerializer, Serializer<V> valueSerializer, Function<K, Integer> sizeOfKey, Function<V, Integer> sizeOfValue) {
        this(keySerializer, valueSerializer, sizeOfKey, sizeOfValue, HashMap::new);
    }

    public MapSerializer(Serializer<K> keySerializer, Serializer<V> valueSerializer, Function<K, Integer> sizeOfKey, Function<V, Integer> sizeOfValue, Supplier<Map<K, V>> instanceSupplier) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.sizeOfKey = sizeOfKey;
        this.sizeOfValue = sizeOfValue;
        this.instanceSupplier = instanceSupplier;
    }

    //practical but not very fast
    @Override
    public ByteBuffer toBytes(Map<K, V> data) {
        int mapByteSize = sizeOfMap(data);
        ByteBuffer bb = ByteBuffer.allocate(Integer.BYTES + mapByteSize);
        writeTo(data, bb);
        return bb.flip();
    }

    @Override
    public void writeTo(Map<K, V> data, ByteBuffer dest) {
        dest.putInt(data.size());

        for (Map.Entry<K, V> entry : data.entrySet()) {
            dest.put(keySerializer.toBytes(entry.getKey()));
            dest.put(valueSerializer.toBytes(entry.getValue()));
        }
    }

    @Override
    public Map<K, V> fromBytes(ByteBuffer buffer) {
        Map<K, V> kvMap = instanceSupplier.get();

        int size = buffer.getInt();
        for (int i = 0; i < size; i++) {
            K key = keySerializer.fromBytes(buffer);
            V value = valueSerializer.fromBytes(buffer);
            kvMap.put(key, value);
        }
        return kvMap;
    }

    public int sizeOfMap(Map<K, V> map) {
        return sizeOfMap(map, sizeOfKey, sizeOfValue);
    }

    public static <K, V> int sizeOfMap(Map<K, V> map, Function<K, Integer> sizeOfKey, Function<V, Integer> sizeOfValue) {
        int size = 0;
        for (Map.Entry<K, V> entry : map.entrySet()) {
            size += sizeOfKey.apply(entry.getKey()) + sizeOfValue.apply(entry.getValue());
        }
        return size;
    }


}
