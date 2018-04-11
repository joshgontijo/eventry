package io.joshworks.lsmtree;

import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class MemTable<K, V> {

    private final SortedMap<K, V> map = new TreeMap<>();

    public V put(K key, V value) {
        return map.put(key, value);
    }

    public V remove(K key) {
        return map.remove(key);
    }

    public V get(K key) {
        return map.get(key);
    }

    public Iterator<Map.Entry<K, V>> iterator() {
        return map.entrySet().iterator();
    }
}
