package io.joshworks.fstore.index.btrees;

import java.util.Iterator;

public interface Tree<K extends Comparable<K>, V> extends Iterable<Entry<K, V>> {

    boolean put(K key, V value);

    V get(K key);

    void clear();

    boolean isEmpty();

    Iterator<Entry<K, V>> iterator();

    int height();

    long size();

    V remove(K key);
}
