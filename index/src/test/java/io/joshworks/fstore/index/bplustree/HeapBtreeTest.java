package io.joshworks.fstore.index.bplustree;

import io.joshworks.fstore.index.bplustree.storage.HeapBlockStore;

public class HeapBtreeTest extends BPlusTreeTestBase {

    @Override
    protected BPlusTree<Integer, String> create(int order) {
        return BPlusTree.of(new HeapBlockStore<Integer, String>(), order);
    }
}
