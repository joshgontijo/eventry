package io.joshworks.fstore.index.bplustree;

import io.joshworks.fstore.index.Entry;
import io.joshworks.fstore.index.Range;
import io.joshworks.fstore.index.bplustree.storage.BlockStore;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class TreeIterator<K extends Comparable<K>, V> implements Iterator<Entry<K, V>> {

    protected final BlockStore store;

    //state
    private BPlusTree.LeafNode currentLeaf;
    protected List<Entry<K, V>> leafEntries;
    protected int positionInLeaf;// using list instead iterator so RangeIterator can read ahead
    private int processedCount;
    private final Range<K> range;

    protected TreeIterator(BlockStore store, int rootId, Range<K> range) {
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

    public static <K extends Comparable<K>, V> TreeIterator<K, V> iterator(BlockStore store, int rootId) {
        return iterator(store, rootId, new Range<K>());
    }

    public static <K extends Comparable<K>, V> TreeIterator<K, V> iterator(BlockStore store, int rootId, Range<K> range) {
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
        BPlusTree.Node next = store.readBlock(currentLeaf.next());
        if (next.type == BPlusTree.Node.INTERNAL_NODE) {
            throw new IllegalStateException("Corrupted index");
        }

        currentLeaf = (BPlusTree.LeafNode) next;
        leafEntries = ((BPlusTree.LeafNode) next).entries();
        positionInLeaf = 0;
        return true;
    }

    private BPlusTree.LeafNode startLeaf(int rootId) {
        BPlusTree.Node node = store.readBlock(rootId);
        if (range.startInclusive() != null) {
            while (node.type == BPlusTree.Node.INTERNAL_NODE) {
                node = ((BPlusTree.InternalNode) node).getChild(range.startInclusive());
                if (node == null) {
                    throw new IllegalStateException("Corrupted index");
                }
            }
            return (BPlusTree.LeafNode) node;
        }

        while (node.type == BPlusTree.Node.INTERNAL_NODE) {
            int childId = (int) ((BPlusTree.InternalNode) node).children.get(0);
            if (childId < 0) {
                throw new IllegalStateException("Corrupted index");
            }
            node = store.readBlock(childId);
        }
        return (BPlusTree.LeafNode) node;
    }
}
