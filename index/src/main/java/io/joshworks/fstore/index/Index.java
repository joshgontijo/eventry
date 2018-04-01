package io.joshworks.fstore.index;

public interface Index<K extends Comparable<K>, V> extends Iterable<Entry<K, V>> {

    V get(K key);

    V put(K key, V value);

    V delete(K key);

    void clear();



}
