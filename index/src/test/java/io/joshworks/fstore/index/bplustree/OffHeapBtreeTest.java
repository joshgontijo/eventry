package io.joshworks.fstore.index.bplustree;

import io.joshworks.fstore.serializer.StandardSerializer;

public class OffHeapBtreeTest extends BPlusTreeTestBase {
    @Override
    protected BPlusTree<Integer, String> create(int order) {
        return BPlusTree.offHeapTree(order, StandardSerializer.INTEGER, StandardSerializer.VSTRING);
    }
}
