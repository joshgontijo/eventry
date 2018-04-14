package io.joshworks.fstore.index.bplustree.storage;


import io.joshworks.fstore.index.bplustree.BPlusTree;

public interface BlockStore {
    void clear();

    int placeBlock(BPlusTree.Node block);

    void freeBlock(int i);

    BPlusTree.Node readBlock(int blockId);

    void writeBlock(int blockId, BPlusTree.Node block);
}
