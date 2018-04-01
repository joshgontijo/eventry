package io.joshworks.fstore.index.btrees.bplustree;


import io.joshworks.fstore.index.Entry;
import io.joshworks.fstore.index.btrees.bplustree.util.DeleteResult;
import io.joshworks.fstore.index.btrees.bplustree.util.InsertResult;
import io.joshworks.fstore.index.btrees.storage.Block;
import io.joshworks.fstore.index.btrees.storage.BlockStore;

import java.util.ArrayList;
import java.util.List;

public abstract class Node<K extends Comparable<K>, V> implements Block {

    protected final BlockStore<Node<K, V>> store;

    private int id = -1;
    protected final int order;
    protected final List<K> keys;

    protected Node(BlockStore<Node<K, V>> store, int order) {
        this.store = store;
        this.order = order;
        this.keys = new ArrayList<>(order - 1);
    }

    public static <K extends Comparable<K>, V> InternalNode<K, V> allocateInternal(BlockStore<Node<K, V>> store, int order) {
        return new InternalNode<>(store, order);
    }

    public static <K extends Comparable<K>, V> LeafNode<K, V> allocateLeaf(BlockStore<Node<K, V>> store, int order) {
        return new LeafNode<>(store, order);
    }

    abstract V getValue(K key);

    abstract DeleteResult<V> deleteValue(K key, Node<K, V> root);

    abstract InsertResult<V> insertValue(K key, V value, Node<K, V> root);

    abstract void merge(Node<K, V> sibling);

    abstract Node<K, V> split();

    abstract boolean isOverflow();

    abstract boolean isUnderflow();

    abstract Entry<K, V> getFirstEntry();

    @Override
    public int id() {
        return id;
    }

    @Override
    public void id(int id) {
        this.id = id;
    }

    public String toString() {
        return keys.toString();
    }

    protected int keyNumber() {
        return keys.size();
    }
}
