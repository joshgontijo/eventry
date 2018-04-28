package io.joshworks.fstore.index.bplustree;

import io.joshworks.fstore.index.Entry;
import io.joshworks.fstore.index.Range;
import io.joshworks.fstore.index.bplustree.storage.BlockStore;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class TreeIterator<K extends Comparable<K>, V> implements Iterator<Entry<K, V>> {

    protected final BlockStore<K, V> store;

    //state
    private LeafNode<K, V> currentLeaf;
    protected List<Entry<K, V>> leafEntries;
    protected int positionInLeaf;// using list instead iterator so RangeIterator can read ahead
    private int processedCount;
    private final Range<K> range;

    protected TreeIterator(BlockStore<K, V> store, int rootId, Range<K> range) {
        this.store = store;
        this.range = range;

        this.currentLeaf = startLeaf(rootId);
        this.leafEntries = currentLeaf.entries();
        this.positionInLeaf = initialLeafPosition();

        for (int i = 0; i < range.skip(); i++) {
            positionInLeaf++;
            if (positionInLeaf >= leafEntries.size())
                loadNextLeaf();
        }
    }

    protected int initialLeafPosition() {
        if (range.startInclusive() != null) {
            int keyIndex = Collections.binarySearch(leafEntries, Entry.of(range.startInclusive(), null)); //entry is key only comparable, so it's ok
            return keyIndex < 0 ? 0 : keyIndex; //initial position with offset within the initial leaf
        }
        return 0;
    }

    public static <K extends Comparable<K>, V> TreeIterator<K, V> iterator(BlockStore<K, V> store, int rootId) {
        return iterator(store, rootId, new Range<>());
    }

    public static <K extends Comparable<K>, V> TreeIterator<K, V> iterator(BlockStore<K, V> store, int rootId, Range<K> range) {
        return new TreeIterator<>(store, rootId, range);
    }

    @Override
    public boolean hasNext() {
        if (currentLeaf == null) {
            return false;
        }
        if (range.limit() > 0 && processedCount >= range.limit()) {
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
        return b && (range.endExclusive() == null || leafEntries.get(positionInLeaf).key.compareTo(range.endExclusive()) < 0);
    }

    @Override
    public Entry<K, V> next() {
        if (positionInLeaf >= leafEntries.size() && currentLeaf.next() >= 0) {
            if (!loadNextLeaf()) {
                throw new IllegalStateException("Premature finalization, could not read node");
            }
        }
        processedCount++;
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
        if (!(next instanceof LeafNode)) {
            throw new IllegalStateException("Corrupted index");
        }

        currentLeaf = (LeafNode<K, V>) next;
        leafEntries = ((LeafNode<K, V>) next).entries();
        positionInLeaf = 0;
        return true;
    }

    private LeafNode<K, V> startLeaf(int rootId) {
        Node<K, V> node = store.readBlock(rootId);
        if (range.startInclusive() != null) {
            while (node instanceof InternalNode) {
                node = ((InternalNode<K, V>) node).getChild(range.startInclusive());
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
