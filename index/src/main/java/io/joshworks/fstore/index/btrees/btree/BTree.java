package io.joshworks.fstore.index.btrees.btree;

import io.joshworks.fstore.index.Entry;
import io.joshworks.fstore.index.btrees.Tree;
import io.joshworks.fstore.index.btrees.storage.BlockStore;

import java.util.Arrays;
import java.util.Iterator;

public class BTree<K extends Comparable<K>, V> implements Tree<K, V> {

    private BlockStore<Node<K, V>> store;

    public final int order;
    private long size;
    private int height;

    int rootId;


    //order n. create minimum key in a non allocate node
    private BTree(BlockStore<Node<K, V>> store, int order) {
        if (order < 2) {
            throw new IllegalArgumentException("order must be equals or greater than 2");
        }

        //TODO
        //If the keys are 4 byte integers and the node indices are also 4 bytes, then setting B=256 means that each node stores
        //(4+4) x 2B = 8 x 512 = 4096

        this.order = order;
        this.store = store;


        rootId = allocateRoot();
    }

    public static <K extends Comparable<K>, V> BTree<K, V> create(BlockStore<Node<K, V>> store) {
        return create(store, 2);
    }

    public static <K extends Comparable<K>, V> BTree<K, V> create(BlockStore<Node<K, V>> store, int t) {
        return new BTree<>(store, t);
    }

    @Override
    public V put(K key, V value) {
        V old = this.insert(Entry.of(key, value));
        if(old == null)
            size++;
        return old;
    }

    @Override
    public V get(K key) {
        return search(rootId, key);
    }

    @Override
    public void clear() {
        store.clear();
        rootId = allocateRoot();
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        if(isEmpty()) {
            return TreeIterator.empty();
        }
        return TreeIterator.fullIterator(rootId, store);
    }

    public Iterator<Entry<K, V>> iterator(K startInclusive) {
        if(isEmpty()) {
            return TreeIterator.empty();
        }
        return TreeIterator.rangeIterator(rootId, store, startInclusive);
    }

    public Iterator<Entry<K, V>> iterator(K startInclusive, K endExclusive) {
        if(isEmpty()) {
            return TreeIterator.empty();
        }
        return TreeIterator.rangeIterator(rootId, store, startInclusive, endExclusive);
    }

    public Iterator<Entry<K, V>> limitIterator(int limit) {
        if(isEmpty()) {
            return TreeIterator.empty();
        }
        return TreeIterator.limitIterator(rootId, store, limit);
    }

    public Iterator<Entry<K, V>> limitIterator(K startInclusive, int limit) {
        if(isEmpty()) {
            return TreeIterator.empty();
        }
        return TreeIterator.limitIterator(rootId, store, startInclusive, limit);
    }

    private int allocateRoot() {
        Node<K, V> node = Node.allocate(order, store);
        node.leaf = true;
        node.root = true;
        node.n = 0;

        store.writeBlock(node.id(), node);

        return node.id();
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
        V removed = remove(root, key);
        if (removed != null) {
            if (root.n == 0 && height > 0) { //root has no entries and only one child, child becomes the new root
                store.freeBlock(root.id());
                height--;
                int childId = root.children[0];

                Node<K, V> child = store.readBlock(childId);
                child.root = true;
                store.writeBlock(child.id(), child);
                rootId = child.id();

            }
            size--;
        }
        return removed;
    }

    private V remove(Node<K, V> node, K key) {

        int i = Search.binarySearch(node, key);

        //not found in this node
        if (i >= 0) { //key not found, go to the next
            if (node.leaf) {
                return null; //not found
            }
            Node<K, V> child = stepInto(node, i);
            return remove(child, key);
        }

        //found
        i = -(i + 1); //the actual index is -i - 1 (ex: if 0, then -1, if 1 then -2)
        if (node.isLeaf()) {
            if (node.n >= order || node.root) {//current node is not minimal or is a root
                Entry<K, V> deleted = node.deleteEntry(i);
                return deleted.value;
            } else {
                throw new IllegalStateException("Leaf must never be minimal when traversing until it");
            }

        } else { //internal

            //find predecessor subtree
            int predecessorId = node.children[i];
            Node<K, V> leftChild = store.readBlock(predecessorId);
            if (leftChild.n >= order) { //TODO NOT SURE IF THIS IS CORRECT (2a / 2b cases)
                while (!leftChild.leaf) {
                    leftChild = stepInto(leftChild, leftChild.n); //go right all the way to the leaf
                }
                Entry<K, V> predecessorKey = leftChild.deleteEntry(leftChild.n - 1);
                Entry<K, V> deleted = node.replaceEntry(predecessorKey, i); //do not use deleteEntry we want to REPLACE
                return deleted.value;

            }
            //find successor subtree
            int successorId = node.children[i + 1];
            Node<K, V> rightChild = store.readBlock(successorId);
            if (rightChild.n >= order) {
                while (!rightChild.leaf) {
                    rightChild = stepInto(rightChild, 0); //go left all the way to the leaf
                }
                Entry<K, V> successorKey = rightChild.deleteEntry(0);
                Entry<K, V> deleted = node.replaceEntry(successorKey, i); //do not use deleteEntry we want to REPLACE
                return deleted.value;
            }
            //Otherwise, if both y and z have only t - 1 entries, merge k and all of z into y,
            //so that x loses both k and the pointer to z, and y now contains 2t - 1 entries.
            //Then free z and recursively delete k from y.

            //both child are minimal, merge them
            leftChild.addEntry(node.deleteEntry(i)); //push key down
            leftChild.merge(rightChild);

            node.deleteChild(i + 1); //parent lost its link to left sibling since its being merged
            return remove(leftChild, key);
        }
    }

    private Node<K, V> stepInto(Node<K, V> node, int i) {
        //TODO this might be an unnecessary block read
        int childId = node.children[i];
        Node<K, V> nextChild = store.readBlock(childId);
        if (nextChild.n >= order) { // nextChild has at least t entries we can step into
            return nextChild;
        }

        //############### 3a ###############
        //nextChild has t - 1, we need to shuffle entries
        Node<K, V> leftSibling = null;
        Node<K, V> rightSibling = null;
        if (i > 0) { //has left sibling
            leftSibling = store.readBlock(node.children[i - 1]);
            if (leftSibling.n >= order) { //has more than than the minimum
                nextChild.shiftRight();
                //move child pointer from leftSibling to nextChild, must come before leftSibling.deleteEntry
                nextChild.children[0] = leftSibling.deleteChild(leftSibling.n);
                Entry<K, V> fromSibling = leftSibling.deleteEntry(leftSibling.n - 1);
                Entry<K, V> fromParent = node.replaceEntry(fromSibling, i - 1);
                nextChild.replaceEntry(fromParent, 0);
                nextChild.n++;
                return nextChild;

            }
        }
        if (i < node.n) { //has right sibling
            rightSibling = store.readBlock(node.children[i + 1]);
            if (rightSibling.n >= order) { //has more than than the minimum

                Entry<K, V> fromSibling = rightSibling.deleteEntry(0);
                Entry<K, V> fromParent = node.replaceEntry(fromSibling, i);
                nextChild.addEntry(fromParent);
                nextChild.addChild(rightSibling.deleteChild(0));

                return nextChild;
            }
        }

        //############### 3b ###############
        //merge left sibling into nextChild (right node)
        if (leftSibling != null) {

            leftSibling.addEntry(node.deleteEntry(i - 1)); //push key down
            leftSibling.merge(nextChild);

            //parent lost its link to leftSibling since its being merged
            //here we actually delete the nextChild pointer because the merge order. (it's easier to merge from right into left)
            //so we simply change the pointer
            node.children[i] = node.children[i - 1];
            node.deleteChild(i - 1); //parent lost its link to left sibling since its being merged

            return leftSibling; //because we swapped the nodes for easy merging, we return the leftSibling node instead
        }
        //merge right sibling into nextChild (left node)
        if (rightSibling != null) {

            nextChild.addEntry(node.deleteEntry(i)); //push key down
            nextChild.merge(rightSibling);
            node.deleteChild(i + 1); //parent lost its link to rightSibling since its being merged

            return nextChild;
        }

        throw new IllegalStateException("TODO - remove me");
    }


    private V search(int nodeId, K key) {
        Node<K, V> x = store.readBlock(nodeId);
        int i = Search.binarySearch(x, key);
        if (i < 0) { //found
            i = -(i + 1); //the actual index is -i - 1 (ex: if 0, then -1, if 1 then -2)
            return x.entries[i].value;
        }

        if (x.leaf)
            return null; // not found

        int childId = x.children[i];
        return search(childId, key);
    }

    private V insert(Entry<K, V> entry) {
        Node<K, V> r = store.readBlock(rootId);
        if (r.isFull()) {
            Node<K, V> s = Node.allocate(order, store);
            rootId = s.id();
            s.leaf = false;

            s.children[0] = r.id();
            splitChild(s, 0, r);
            V inserted = insertNonFull(s, entry);
            if (inserted == null) { //if not null, the value was simply replaced, no restructure happened
                height++;
                s.height = height;

                r.root = false;
                s.root = true;
            }
            return inserted;
        }
        return insertNonFull(r, entry);
    }

    private V insertNonFull(Node<K, V> x, Entry<K, V> entry) {
        int i = Search.binarySearch(x, entry.key);

        if (x.leaf) {
            if(i < 0) { //Entry already exist, replace it
                i = -(i + 1);
                return x.replaceEntry(entry, i).value;
            }
            if(i >= x.n) { // no need to shift entries
                x.addEntry(entry);
            } else {
                System.arraycopy(x.entries, i, x.entries, i + 1, x.entries.length - i - 1);
                x.replaceEntry(entry, i);
                x.n++;
            }

            store.writeBlock(x.id(), x);
            return null;

        } else {

            Node<K, V> child = store.readBlock(x.children[i]);
            if (child.isFull()) {
                Node<K, V> rightNode = splitChild(x, i, child);//call split on node x's ith child

                if (entry.compareTo(x.entries[i]) > 0) {// if after the split we need to change direction, we do it here
                    child = rightNode;
                }
            }
            return insertNonFull(child, entry);//recurse
        }
    }

    private Node<K, V> splitChild(Node<K, V> x, int i, Node<K, V> y) {

        Node<K, V> z = Node.allocate(order, store);

        // new x is a leaf if old x was
        z.leaf = y.leaf;
        z.height = y.height;
        // since y is full, the new x must have t-1 entries
        z.n = order - 1;

//        z.splitInto(z);
        // copy over the "right half" create y into z
        System.arraycopy(y.entries, order, z.entries, 0, order - 1);
        Arrays.fill(y.entries, order, y.entries.length, null);

        // copy over the child pointers if y isn't a leaf
        if (!y.leaf) {
            System.arraycopy(y.children, order, z.children, 0, order);
            Arrays.fill(y.children, order, y.children.length, -1);
        }

        // having "chopped off" the right half create y, it now has t-1 entries
        y.n = order - 1;

        // shift everything in x over from i+1, then stick the new y in x;
        // y will half its former self as ci[x] and z will
        // be the other half as ci+1[x]
        System.arraycopy(x.children, i + 1, x.children, i + 1 + 1, x.n - i);
        x.children[i + 1] = z.id();

        System.arraycopy(x.entries, i, x.entries, i + 1, x.n - i);

        x.entries[i] = y.entries[order -1];
        y.entries[order -1] = null;

        x.n++;

        store.writeBlock(y.id(), y); //left
        store.writeBlock(z.id(), z); //right
        store.writeBlock(x.id(), x); //parent


        return z;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        toString(rootId, sb);
        return sb.toString();
    }

    /**
     * A recursive algorithm for converting this tree into a string
     *
     * @param ui the subtree to add to the the string
     * @param sb a StringBuffer for building the string
     */
    private void toString(int ui, StringBuffer sb) {
        if (ui < 0) return;
        Node u = store.readBlock(ui);
        if (u == null) {
            throw new RuntimeException();
        }
        int i = 0;
        while (i < order && u.entries[i] != null) {
            toString(u.children[i], sb);
            sb.append(u.entries[i++] + ", ");
        }
        toString(u.children[i], sb);
    }

}