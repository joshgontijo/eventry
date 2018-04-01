package io.joshworks.fstore.index.btrees.btree;

import io.joshworks.fstore.index.Entry;
import io.joshworks.fstore.index.btrees.storage.Block;
import io.joshworks.fstore.index.btrees.storage.BlockStore;

import java.lang.reflect.Array;
import java.text.MessageFormat;
import java.util.Arrays;

public class Node<K extends Comparable<K>, V> implements Block {

    //min entries: t - 1
    //max entries: 2t - 1
    //min children: t + 1
    //max children: 2t
    int order;
    boolean leaf;
    boolean root;
    int height; //leaf=0 root=tree.size

    private int id;


    final Entry<K, V>[] entries;
    int n; //number create entries

    final int[] children; //-1 when no children reference


    private BlockStore<Node<K, V>> store;


    @SuppressWarnings("unchecked")
    private Node(int order, BlockStore<Node<K, V>> store, boolean leaf) {
        this.order = order;
        this.entries = (Entry<K, V>[]) Array.newInstance(Entry.class, 2 * order -1);
        if(!leaf){
            this.children = leaf ? null : new int[2 * order];
            Arrays.fill(children, 0, children.length, -1);
        } else  {
            children = null;
        }
        this.store = store;
    }

    static <K extends Comparable<K>, V> Node<K, V> allocate(int order, BlockStore<Node<K, V>> store) {
        Node<K, V> node = new Node<>(order, store, false);
        node.id = store.placeBlock(node);
        return node;
    }

    static <K extends Comparable<K>, V> Node<K, V> allocateLeaf(int order, BlockStore<Node<K, V>> store) {
        Node<K, V> node = new Node<>(order, store, true);
        node.id = store.placeBlock(node);
        return node;
    }

    public int id() {
        return id;
    }

    @Override
    public void id(int id) {
        this.id = id;
    }

    boolean isFull() {
        return (2 * order - 1) == n;
    }

    boolean isLeaf() {
        return childCount() == 0;
    }

    int childCount() {
        int childCount = 0;
        for (int child : children) {
            if (child >= 0)
                childCount++;
        }
        return childCount;
    }

    boolean isInternal() { //root can also be internal
        return childCount() > 0;
    }

    void merge(Node other) {
        System.arraycopy(other.entries, 0, entries, n, other.n);
        System.arraycopy(other.children, 0, children, n, other.n + 1);
        store.freeBlock(other.id());

        n += other.n;
    }

    /**
     * Split this node into two nodes
     *
     * @return the newly created block, which has the larger entries
     */
    protected Node<K, V> split() {
        Node<K, V> z = Node.allocate(order, store);
        int j = entries.length / 2;
        System.arraycopy(entries, j, z.entries, 0, entries.length - j);
        Arrays.fill(entries, j, entries.length, null);

        if (!leaf) {
            System.arraycopy(children, j + 1, z.children, 0, children.length - j - 1);
            Arrays.fill(children, j + 1, children.length, -1);
        }

        z.height = height;
        z.leaf = leaf;
        z.n = entries.length - j;
        n = j;

        store.writeBlock(z.id, z);
        return z;
    }

    void addEntry(Entry<K, V> entry) {
        if (n > 2 * order - 1) {
            throw new IllegalStateException("Reached maximum entries on this node (2t-1). Key count: " + n);
        }
        if (n >= 1 && entry.compareTo(entries[n - 1]) <= 0) {
            throw new IllegalArgumentException(MessageFormat.format("Key {0} must be greater than previous key in the node: {1}", entry, entries[n - 1]));
        }
        entries[n++] = entry;
    }

    Entry<K, V> replaceEntry(Entry<K, V> entry, int idx) {
        if (idx >= n) {
            throw new IllegalArgumentException(MessageFormat.format("Key {0} must be added in sequence with previous element in the node, node size {1}, insertion index {2}", entry, n, idx));
        }
        Entry<K, V> old = entries[idx];
        if (old == null) {
            throw new IllegalStateException(MessageFormat.format("Key at index {0} cannot be null", idx));
        }
        entries[idx] = entry;
        return old;
    }

    void shiftRight() {
        System.arraycopy(entries, 0, entries, 1, n); //shift entries right
        System.arraycopy(children, 0, children, 1, n + 1); //shift children right
    }

    Entry<K, V> deleteEntry(int i) {
        Entry<K, V> entry = entries[i];
        //shift left remaining elements
        System.arraycopy(entries, i + 1, entries, i, n - i - 1);
        entries[--n] = null;
        return entry;
    }

    int deleteChild(int i) {
        int c = children[i];
        //shift left remaining elements
        System.arraycopy(children, i + 1, children, i, children.length - i - 1);
        children[children.length - 1] = -1;
        return c;
    }

    void addChild(int childId) {
        children[n] = childId;
    }

    void updateChild(int childId, int idx) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public String toString() {
//        return Arrays.toString(entries) + " - ID: " + id + " H:" + height + " N:" + n;
        StringBuilder sb = new StringBuilder();
        sb.append("{").append(id).append("} ");
        for (int i = 0; i < n; i++) {
            sb.append(" (").append(children[i]).append(") ").append(entries[i]);
        }
        sb.append(" (").append(children[n]).append(")");
        return sb.toString();
    }

    public String toString(Node<K, V> node) {
        StringBuilder s = new StringBuilder();

        s.append(node).append("\n");
        for (int c : node.children) {
            if (c >= 0) {
                Node<K, V> child = store.readBlock(c);
                s.append(child.toString(child));
            }
        }
        return s.toString();
    }

}