package io.joshworks.fstore.index.btrees.bplustree;


import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.index.Entry;
import io.joshworks.fstore.index.btrees.bplustree.util.DeleteResult;
import io.joshworks.fstore.index.btrees.bplustree.util.InsertResult;
import io.joshworks.fstore.index.btrees.storage.Block;
import io.joshworks.fstore.index.btrees.storage.BlockStore;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public abstract class Node<K extends Comparable<K>, V> implements Block {

    protected final BlockStore<Node<K, V>> store;

    protected static final int LEAF_NODE = 0;
    protected static final int INTERNAL_NODE = 1;

    private int id = -1;
    protected final int order;
    protected final int type;
    protected final List<K> keys;

    protected Node(BlockStore<Node<K, V>> store, int order, int type) {
        this.store = store;
        this.order = order;
        this.keys = new ArrayList<>(order - 1);
        this.type = type;
    }

    static <K extends Comparable<K>, V> InternalNode<K, V> allocateInternal(BlockStore<Node<K, V>> store, int order) {
        return new InternalNode<>(store, order);
    }

    static <K extends Comparable<K>, V> LeafNode<K, V> allocateLeaf(BlockStore<Node<K, V>> store, int order) {
        return new LeafNode<>(store, order);
    }

    protected abstract Serializer<Node<K, V>> serializer(Serializer<K> keySerializer, Serializer<V> valueSerializer);

    public static <K extends Comparable<K>, V> Node<K, V> fromBytes(Serializer<K> keySerializer, Serializer<V> valueSerializer, ByteBuffer data) {
        //get node type and invoke the appropriate serializer
        return null;
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
