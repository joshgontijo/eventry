package io.joshworks.fstore.index;

import java.util.Map;

public interface Index<K extends Comparable<K>, V> extends Iterable<Map.Entry<K, V>> {

    V get(K key);

    V put(K key, V value);

    V delete(K key);

    void clear();



}
