package io.joshworks.fstore.index.btrees.bplustree;

import io.joshworks.fstore.index.btrees.Entry;
import io.joshworks.fstore.index.btrees.storage.BlockStore;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class TreeIterator<K extends Comparable<K>, V> implements Iterator<Entry<K, V>> {

    protected static final int NO_LIMIT = -1;
    protected static final int SKIP_NONE = 0;

    protected final BlockStore<Node<K, V>> store;
    private final int limit;

    //state
    private LeafNode<K, V> currentLeaf;
    protected List<Entry<K, V>> leafEntries;
    protected int positionInLeaf;// using list instead iterator so RangeIterator can read ahead
    private int proccessedCount;
    private final K startInclusive;
    private final K endExclusive;

    protected TreeIterator(
            BlockStore<Node<K, V>> store,
            int rootId,
            int skip,
            int limit,
            K startInclusive,
            K endExclusive) {
        this.store = store;
        this.limit = limit;
        this.startInclusive = startInclusive;
        this.endExclusive = endExclusive;

        this.currentLeaf = startLeaf(rootId);
        this.leafEntries = currentLeaf.entries();
        this.positionInLeaf = initialLeafPosition(currentLeaf.entries());

        for (int i = 0; i < skip; i++) {
            positionInLeaf++;
            if (positionInLeaf >= leafEntries.size())
                loadNextLeaf();
        }
    }

    protected int initialLeafPosition(List<Entry<K, V>> entries) {
        if (startInclusive != null) {
            int keyIndex = Collections.binarySearch(leafEntries, Entry.of(startInclusive, null)); //entry is key only comparable, so it's ok
            return keyIndex < 0 ? 0 : keyIndex; //initial position with offset within the initial leaf
        }
        return 0;
    }

    public static <K extends Comparable<K>, V> TreeIterator<K, V> iterator(BlockStore<Node<K, V>> store, int rootId) {
        return new TreeIterator<>(store, rootId, SKIP_NONE, NO_LIMIT, null, null);
    }

    public static <K extends Comparable<K>, V> TreeIterator<K, V> rangeIterator(BlockStore<Node<K, V>> store, int rootId, K startInclusive, K endExclusive) {
        return new TreeIterator<>(store, rootId, SKIP_NONE, NO_LIMIT, startInclusive, endExclusive);
    }

    public static <K extends Comparable<K>, V> TreeIterator<K, V> limitIterator(BlockStore<Node<K, V>> store, int rootId, int skip, int limit) {
        return new TreeIterator<>(store, rootId, skip, limit, null, null);
    }

    public static <K extends Comparable<K>, V> TreeIterator<K, V> limitIterator(BlockStore<Node<K, V>> store, int rootId, int skip, int limit, K startInclusive, K endExclusive) {
        return new TreeIterator<>(store, rootId, skip, limit, startInclusive, endExclusive);
    }

    @Override
    public boolean hasNext() {
        if (currentLeaf == null) {
            return false;
        }
        if (limit > 0 && proccessedCount >= limit) {
            return false;
        }
        if (positionInLeaf < leafEntries.size()) {
            return true;
        }
        if (currentLeaf.next() < 0) {
            return false;
        }
        boolean leafLoaded = loadNextLeaf();
        boolean b = leafLoaded && !leafEntries.isEmpty();
        return b && (endExclusive == null || leafEntries.get(positionInLeaf).key.compareTo(endExclusive) < 0);
    }

    @Override
    public Entry<K, V> next() {
        if (positionInLeaf >= leafEntries.size() && currentLeaf.next() >= 0) {
            if (!loadNextLeaf()) {
                throw new IllegalStateException("Premature finalization, could not read node");
            }
        }
        proccessedCount++;
        return leafEntries.get(positionInLeaf++);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    private boolean loadNextLeaf() {
        if (currentLeaf.next() < 0) {
            return false;
        }
        Node<K, V> next = store.readBlock(currentLeaf.next());
        if (next == null || !(next instanceof LeafNode)) {
            throw new IllegalStateException("Corrupted index");
        }

        currentLeaf = (LeafNode<K, V>) next;
        leafEntries = ((LeafNode<K, V>) next).entries();
        positionInLeaf = 0;
        return true;
    }

    private LeafNode<K, V> startLeaf(int rootId) {
        Node<K, V> node = store.readBlock(rootId);
        if (startInclusive != null) {
            while (node instanceof InternalNode) {
                node = ((InternalNode<K, V>) node).getChild(startInclusive);
                if (node == null) {
                    throw new IllegalStateException("Corrupted index");
                }
            }
            return (LeafNode<K, V>) node;
        }

        while (node instanceof InternalNode) {
            int childId = ((InternalNode<K, V>) node).children.get(0);
            if (childId < 0) {
                throw new IllegalStateException("Corrupted index");
            }
            node = store.readBlock(childId);
        }
        return (LeafNode<K, V>) node;
    }
}
