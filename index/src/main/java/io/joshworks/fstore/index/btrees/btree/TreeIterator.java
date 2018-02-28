package io.joshworks.fstore.index.btrees.btree;


import io.joshworks.fstore.index.btrees.Entry;
import io.joshworks.fstore.index.btrees.storage.BlockStore;

import java.util.Iterator;
import java.util.LinkedList;

public class TreeIterator<K extends Comparable<K>, V> implements Iterator<Entry<K, V>> {
    private final LinkedList<Node<K, V>> nodeStack = new LinkedList<>();
    private final LinkedList<Integer> idStack = new LinkedList<>();
    private final BlockStore<Node<K, V>> bs;
    private final K endExclusive;

    protected TreeIterator(int rootId, BlockStore<Node<K, V>> bs) {
        this.bs = bs;
        this.endExclusive = null;
        loadStack(rootId);
    }

    protected TreeIterator(K startingInclusive, K endExclusive, int rootId, BlockStore<Node<K, V>> bs) {
        this.bs = bs;
        this.endExclusive = endExclusive;

        if (startingInclusive == null) {
            throw new IllegalArgumentException("startingInclusive must no be null");
        }
        if (endExclusive != null && endExclusive.compareTo(startingInclusive) <= 0) {
            throw new IllegalArgumentException("endExclusive must be greater than startingInclusive");
        }

        Node<K, V> u;
        int i;

        int ui = rootId;
        do {
            u = bs.readBlock(ui);
            i = Search.binarySearch(u, startingInclusive);
            if (u.n == 0)
                continue; //do not add nodes without entries
            nodeStack.add(u);
            if (i < 0) {
                idStack.add(-(i + 1));
                return;
            }
            idStack.add(i);
            ui = u.children[i];
        } while (ui >= 0);
        if (i == u.n)
            advance();
    }

    @Override
    public boolean hasNext() {
        if (nodeStack.isEmpty()) {
            return false;
        }
        if (endExclusive == null) {
            return true;
        }
        Node<K, V> u = nodeStack.getLast();
        int i = idStack.getLast();
        Entry<K, V> entry = u.entries[i];
        //we don't want to compare with null values, we skip to the next item
        return entry == null || entry.key.compareTo(endExclusive) < 0;
    }

    @Override
    public Entry<K, V> next() {
        Node<K, V> u = nodeStack.getLast();
        int i = idStack.getLast();
        Entry<K, V> entry = u.entries[i++];
        idStack.set(idStack.size() - 1, i);
        advance();
        return entry;
    }

    private void advance() {
        Node<K, V> u = nodeStack.getLast();
        int i = idStack.getLast();
        if (u.isLeaf()) { // this is a leaf, walk up
            while (!nodeStack.isEmpty() && i == u.n) {
                nodeStack.removeLast();
                idStack.removeLast();
                if (!nodeStack.isEmpty()) {
                    u = nodeStack.getLast();
                    i = idStack.getLast();
                }
            }
        } else { // this is an internal node, walk down
            int ui = u.children[i];
            loadStack(ui);
        }
    }

    private void loadStack(int baseNodeId) {
        int ui = baseNodeId;
        do {
            Node<K, V> u = bs.readBlock(ui);
            ui = u.children[0];
            if (u.n == 0)
                continue; //do not add nodes without entries

            nodeStack.add(u);
            idStack.add(0);
        } while (ui >= 0);
    }


    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }


    static <K extends Comparable<K>, V> Iterator<Entry<K, V>> fullIterator(int rootId, BlockStore<Node<K, V>> store) {
        return new TreeIterator<>(rootId, store);
    }

    static <K extends Comparable<K>, V> Iterator<Entry<K, V>> rangeIterator(int rootId, BlockStore<Node<K, V>> store, K startInclusive) {
        return new TreeIterator<>(startInclusive, null, rootId, store);
    }

    static <K extends Comparable<K>, V> Iterator<Entry<K, V>> rangeIterator(int rootId, BlockStore<Node<K, V>> store, K startInclusive, K endExclusive) {
        return new TreeIterator<>(startInclusive, endExclusive, rootId, store);
    }

    static <K extends Comparable<K>, V> Iterator<Entry<K, V>> limitIterator(int rootId, BlockStore<Node<K, V>> store, int limit) {
        return new LimitTreeIterator<>(rootId, store, limit);
    }

    static <K extends Comparable<K>, V> Iterator<Entry<K, V>> limitIterator(int rootId, BlockStore<Node<K, V>> store, K startInclusive, int limit) {
        return new LimitTreeIterator<>(startInclusive, rootId, store, limit);
    }

    static <K extends Comparable<K>, V> Iterator<Entry<K, V>> empty() {
        return new Iterator<Entry<K, V>>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public Entry<K, V> next() {
                return null;
            }
        };
    }

}