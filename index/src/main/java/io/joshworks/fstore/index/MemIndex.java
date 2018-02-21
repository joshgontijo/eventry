package io.joshworks.fstore.index;

import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class MemIndex<K extends Comparable<K>, V> implements Index<K, V> {

    private final SortedMap<K, V> index = new TreeMap<>();

    @Override
    public V get(K key) {
        checkKey(key);
        return index.get(key);
    }

    @Override
    public V put(K key, V value) {
        checkKey(key);
        return index.put(key, value);
    }

    @Override
    public V delete(K key) {
        checkKey(key);
        return index.remove(key);
    }

    @Override
    public Iterator<Map.Entry<K, V>> iterator() {
        return index.entrySet().iterator();
    }

    private void checkKey(K key) {
        if (key == null) {
            throw new IllegalArgumentException("Key must be provided");
        }
    }
}
