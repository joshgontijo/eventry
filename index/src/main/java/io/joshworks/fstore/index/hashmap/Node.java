package io.joshworks.fstore.index.hashmap;

import java.util.Map;

class Node<K, V> implements Map.Entry<K, V> {

    private final K key;
    private V value;

    private int next;
    private int memory;

    Node(K key) {
       this(key, null);
    }

    Node(K key, V value) {
        this(key, value, -1);
    }

    Node(K key, V value, int next) {
        this.key = key;
        this.value = value;
        this.next = next;
    }

    public void setNext(int next) {
        this.next = next;
    }

    public int getNext() {
        return next;
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public V setValue(V value) {
        V temp = this.value;
        this.value = value;
        return temp;
    }
}
