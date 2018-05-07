package io.joshworks.fstore.es;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.function.Function;

public class LRUCache<K, V> {

    private final Function<K, V> supplier;
    private final int capacity;
    private final LinkedHashMap<K, V> map;

    public LRUCache(int capacity, Function<K, V> supplier) {
        this.capacity = capacity;
        this.supplier = supplier;
        this.map = new LinkedHashMap<>();
    }

    public V get(K key) {
        V value = this.map.get(key);
        if (value != null) {
            this.set(key, value);
        } else {
            V supplied = supplier.apply(key);
            set(key, supplied);
            value = supplied;
        }
        return value;
    }

    public V set(K key, V value) {
        V prev = map.remove(key);
        if (map.size() >= capacity) {
            Iterator<K> it = this.map.keySet().iterator();
            it.next();
            it.remove();
        }
        map.put(key, value);
        return prev;
    }
}