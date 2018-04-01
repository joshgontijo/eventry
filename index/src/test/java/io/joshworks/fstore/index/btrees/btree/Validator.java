package io.joshworks.fstore.index.btrees.btree;

import io.joshworks.fstore.index.Entry;
import io.joshworks.fstore.index.btrees.storage.BlockStore;

import java.text.MessageFormat;

import static org.junit.Assert.fail;

public class Validator {

    public static <K extends Comparable<K>, V> void validateNodes(BlockStore<Node<K, V>> store, int ui, int t) {
        if (ui < 0) return;

        Node<K, V> u = store.readBlock(ui);
        if (u == null) {
            throw new RuntimeException();
        }

        int i = -1;
        while (++i < t && u.entries[i] != null) {
            validateNodes(store, u.children[i], t);
        }
        validateNode(store, u);
    }

    private static <K extends Comparable<K>, V> void validateNode(BlockStore<Node<K, V>> store, Node<K, V> node) {
        //array consistency
        checkKeysCount(node);
        if(!node.isLeaf() && !node.root)
            checkChildCount(node);


        //#1:  Every node other than the root node must have at least (t – 1) entries.  It is the lower bound for the total number create entries in B-Tree’s node.
        if (!node.root && node.n < node.order - 1) {
            fail("Less than t - 1 entries. t=" + node.order + ", n=" + node.n);
        }
        //#2:  Every node including the root node must have at most (2t – 1) entries.  So we say the node is full if it has (2t – 1) entries.  It is the upper bound for the total number create entries in B-Tree’s node.
        if (node.n > 2 * node.order - 1) {
            fail("Greater than 2t - 1 entries. t=" + node.order + ", n=" + node.n);
        }
        //#3:  Every internal node (other than the root node) must have at least t children.
        if (node.isInternal() && !node.root && node.childCount() < node.order) {
            fail("Must have at least t children. t=" + node.order + ", children=" + node.childCount());
        }
        //#4: Every internal node (including the root node) must have at most 2t children.
        if (node.isInternal() && node.childCount() > 2 * node.order) {
            fail("Must have at most 2t children. t=" + node.order + ", children=" + node.childCount());
        }

        //#5:  The entries in a node must be stored in ascending order.  For example in Figure 1, node [12 | 15 | 19 | 26] has key 12 < key 15 < key 19 < key 26
        Entry<K, V> last = null;
        for (int i = 0; i < node.n; i++) {
            if (last == null) {
                last = node.entries[i];
                continue;
            }
            if (last.compareTo(node.entries[i]) >= 0) {
                fail("Keys not in ascending order entries[i-1]=" + last + " entries[i]=" + node.entries[i]);
            }
        }
        //#6:  All the entries of child nodes that are on the left side of a key must be smaller than that key.  In Figure 1, child nodes that are on the left side of key 30 are node [12 | 15 | 19 | 26], node [10 | 11], node [13 | 14] , node [16 | 18], node [20 | 22 | 25] and node [28 | 29] have their entries smaller than 30.
        if (!node.leaf) {
            //since all entries are in ascending order we can just compare the first parent key with the first child key
            Entry<K, V> entry = node.entries[0];
            Node<K, V> child = store.readBlock(node.children[0]);
            if (entry.compareTo(child.entries[0]) <= 0) {
                fail(MessageFormat.format("Parent key {0} must be greater than child first key {1}", String.valueOf(entry), String.valueOf(child.entries[0])));
            }
        }


        //#7:  All the entries create child nodes that are on the right side create a key must be larger than that key.  For example in Figure 1, child nodes that are on the right side create key 9 are node [12 | 15 | 19 | 26], node [10 | 11], node [13 | 14] , node [16 | 18], node [20 | 22 | 25] and node [28 | 29] have their entries larger than 9.
        if (!node.leaf) {
            //since all entries are in ascending order we can just compare the first parent key with the first child key
            Entry<K, V> entry = node.entries[0];
            Node<K, V> child = store.readBlock(node.children[1]);
            if (entry.compareTo(child.entries[0]) >= 0) {
                fail(MessageFormat.format("Parent key {0} must be greater than child first key {1}", String.valueOf(entry), String.valueOf(child.entries[0])));
            }
        }

        //#7:  All leaf nodes must have the same height
        if (node.isLeaf() && node.height != 0) {
            fail("Leaf height must be always zero, leafHeight=" + node.height);
        }

        //#8: Every node with n entries must have exactly x + 1 children
        if (node.isInternal() && node.childCount() != node.n + 1) {
            fail("childCount does not correspond to the right number create children: n + 1. n=" + node.n + ", childCount()=" + node.childCount());
        }
    }

    //checks the consistency of the count and the actual number of entries, to avoid dangling values
    private static <K extends Comparable<K>, V> void checkKeysCount(Node<K, V> node) {
        int nonNullKeys = 0;
        for (Comparable key : node.entries) {
            nonNullKeys = key == null ? nonNullKeys : nonNullKeys + 1;
        }
        if(nonNullKeys != node.n) {
            fail(MessageFormat.format("Non null entries count {0} doesn\'\'t match expected n variable {1}", nonNullKeys, node.n));
        }
    }

    //checks the consistency of the count and the actual number of entries, to avoid dangling values
    private static <K extends Comparable<K>, V> void checkChildCount(Node<K, V> node) {
        int validChildrenCount = 0;
        for (int childId : node.children) {
            validChildrenCount = childId < 0 ? validChildrenCount : validChildrenCount + 1;
        }
        if(node.n + 1 != validChildrenCount) {
            fail(MessageFormat.format("Child count of {0} doesn\'\'t match expected value {1}", validChildrenCount, node.n + 1));
        }
    }

}
