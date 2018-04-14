package io.joshworks.fstore.index.bplustree;

public class HeapBtreeTest extends BPlusTreeTestBase {

    @Override
    protected BPlusTree<Integer, String> create(int order) {
        return BPlusTree.heapTree(order);
    }
}
