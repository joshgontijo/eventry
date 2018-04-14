package io.joshworks.fstore.index.bplustree;

import io.joshworks.fstore.index.Entry;
import io.joshworks.fstore.index.bplustree.storage.BlockStore;
import io.joshworks.fstore.index.bplustree.util.DeleteResult;
import io.joshworks.fstore.index.bplustree.util.InsertResult;

import java.util.ArrayList;
import java.util.List;

public abstract class Node<K extends Comparable<K>, V> {

    public static final int LEAF_NODE = 1;
    public static final int INTERNAL_NODE = 2;

    protected BlockStore<K, V> store;

    private int id = -1;
    final int type;
    final List<K> keys;
    final int order;

    private boolean dirty;


    protected Node(int type, int order, BlockStore<K, V> store) {
        this.keys = new ArrayList<>(order - 1);
        this.type = type;
        this.order = order;
        this.store = store;
    }


    abstract V getValue(K key);

    abstract DeleteResult<V> deleteValue(K key, Node<K, V> root);

    abstract InsertResult<V> insertValue(K key, V value, Node<K, V> root);

    abstract void merge(Node<K, V> sibling);

    abstract Node<K, V> split();

    abstract boolean isOverflow();

    abstract boolean isUnderflow();

    abstract Entry<K, V> getFirstEntry();

    public int id() {
        return id;
    }

    public void id(int id) {
        this.id = id;
    }

    public String toString() {
        return keys.toString();
    }

    protected int keyNumber() {
        return keys.size();
    }

    protected void markDirty() {
        this.dirty = true;
    }

    public boolean dirty() {
        return dirty;
    }

}