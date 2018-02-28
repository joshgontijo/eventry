package io.joshworks.fstore.index.btrees.bplustree;


import io.joshworks.fstore.index.btrees.Entry;
import io.joshworks.fstore.index.btrees.Tree;
import io.joshworks.fstore.index.btrees.bplustree.util.DeleteResult;
import io.joshworks.fstore.index.btrees.bplustree.util.InsertResult;
import io.joshworks.fstore.index.btrees.bplustree.util.Result;
import io.joshworks.fstore.index.btrees.storage.BlockStore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class BPlusTree<K extends Comparable<K>, V> implements Tree<K, V> {

    /**
     * The branching factor used when none specified in constructor.
     */
    private static final int DEFAULT_BRANCHING_FACTOR = 128;

    /**
     * The branching factor for the B+ tree, that measures the capacity of nodes
     * (i.e., the number of children nodes) for internal nodes in the tree.
     */
    final int order;
    private int rootId;
    private final BlockStore<Node<K, V>> store;
    private int size;
    private int height;

    private BPlusTree(BlockStore<Node<K, V>> store, int order) {
        this.order = order;
        this.store = store;
        this.rootId = store.placeBlock(Node.allocateLeaf(store, order));
    }

    public static <K extends Comparable<K>, V> BPlusTree<K, V> of(BlockStore<Node<K, V>> store) {
        return of(store, DEFAULT_BRANCHING_FACTOR);
    }

    public static <K extends Comparable<K>, V> BPlusTree<K, V> of(BlockStore<Node<K, V>> store, int order) {
        if (order <= 2) { //
            throw new IllegalArgumentException("B+Tree order must be greater than 2");
        }
        return new BPlusTree<>(store, order);
    }


    @Override
    public boolean put(K key, V value) {
        Node<K, V> root = store.readBlock(rootId);
        InsertResult insertResult = root.insertValue(key, value, root);
        if (insertResult.newRootId != Result.NO_NEW_ROOT) {
            rootId = insertResult.newRootId;
            height++;
        }
        if (insertResult.inserted)
            size++;
        return insertResult.inserted;
    }

    @Override
    public V get(K key) {
        Node<K, V> root = store.readBlock(rootId);
        return root.getValue(key);
    }

    @Override
    public void clear() {
        store.clear();
        rootId = store.placeBlock(Node.allocateLeaf(store, order));
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        return TreeIterator.iterator(store, rootId);
    }

    public Iterator<Entry<K, V>> range(K startInclusive, K endExclusive) {
        return TreeIterator.rangeIterator(store, rootId, startInclusive, endExclusive);
    }

    public Iterator<Entry<K, V>> limit(int skip, int limit) {
        return TreeIterator.limitIterator(store, rootId, skip, limit);
    }

    public Iterator<Entry<K, V>> limit(int skip, int limit, K startInclusive) {
        return TreeIterator.limitIterator(store, rootId, skip, limit, startInclusive, null);
    }

    @Override
    public int height() {
        return height;
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public V remove(K key) {
        Node<K, V> root = store.readBlock(rootId);
        DeleteResult<V> deleteResult = root.deleteValue(key, root);
        if (deleteResult.newRootId != Result.NO_NEW_ROOT) {
            rootId = deleteResult.newRootId;
            height--;
        }
        if (deleteResult.deleted != null)
            size--;
        return deleteResult.deleted;
    }


    @Override
    public String toString() {
        Node<K, V> root = store.readBlock(rootId);

        Queue<List<Node<K, V>>> queue = new LinkedList<>();
        queue.add(Arrays.asList(root));
        StringBuilder sb = new StringBuilder();
        while (!queue.isEmpty()) {
            Queue<List<Node<K, V>>> nextQueue = new LinkedList<>();
            while (!queue.isEmpty()) {
                List<Node<K, V>> nodes = queue.remove();
                sb.append('{');
                Iterator<Node<K, V>> it = nodes.iterator();
                while (it.hasNext()) {
                    Node<K, V> node = it.next();
                    sb.append(node.toString());
                    if (it.hasNext())
                        sb.append(", ");
                    if (node instanceof InternalNode) {
                        List<Integer> children = ((InternalNode) node).children;

                        List<Node<K, V>> childrenNodes = new ArrayList<>();
                        for (Integer childId : children) {
                            if (childId >= 0)
                                childrenNodes.add(store.readBlock(childId));
                        }
                        nextQueue.add(childrenNodes);
                    }

                }
                sb.append('}');
                if (!queue.isEmpty())
                    sb.append(", ");
                else
                    sb.append('\n');
            }
            queue = nextQueue;
        }

        return sb.toString();
    }
}
