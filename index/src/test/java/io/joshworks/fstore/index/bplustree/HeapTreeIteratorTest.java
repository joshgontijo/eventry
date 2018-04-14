package io.joshworks.fstore.index.bplustree;

public class HeapTreeIteratorTest extends TreeIteratorBase {
    @Override
    protected BPlusTree<Integer, String> create(int order) {
        return BPlusTree.heapTree(order);
    }
}
