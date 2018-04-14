package io.joshworks.fstore.index.bplustree.storage;


import io.joshworks.fstore.index.bplustree.Node;

public interface BlockStore<K extends Comparable<K>, V> {
    void clear();

    int placeBlock(Node<K, V> block);

    void freeBlock(int i);

    Node<K, V> readBlock(int blockId);

    void writeBlock(Node<K, V> block);
}
