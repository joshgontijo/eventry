package io.joshworks.fstore.index.bplustree;

import io.joshworks.fstore.index.Entry;

import java.util.Iterator;

public interface Tree<K extends Comparable<K>, V> extends Iterable<Entry<K, V>> {

    V put(K key, V value);

    V get(K key);

    void clear();

    boolean isEmpty();

    Iterator<Entry<K, V>> iterator();

    int height();

    long size();

    V remove(K key);
}
