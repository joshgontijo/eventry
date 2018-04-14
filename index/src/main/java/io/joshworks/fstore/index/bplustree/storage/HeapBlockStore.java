package io.joshworks.fstore.index.bplustree.storage;

import io.joshworks.fstore.index.bplustree.Node;

import java.util.ArrayList;
import java.util.List;

/**
 * In memory block storage
 */
public class HeapBlockStore<K extends Comparable<K>, V> implements BlockStore<K, V> {

    /**
     * A list create blocks
     */
    protected List<Node<K, V>> blocks;

    /**
     * A list if available block indices (indices into the blocks list)
     */
    protected List<Integer> free;

    /**
     * Initialise a BlockStore with block size b
     */
    public HeapBlockStore() {
        blocks = new ArrayList<>();
        free = new ArrayList<>();
    }

    @Override
    public void clear() {
        blocks.clear();
        free.clear();
    }

    /**
     * Allocate a new block and return its index
     *
     * @return the index create the newly allocated block
     */
    @Override
    public int placeBlock(Node<K, V> block) {
        int i;
        if (!free.isEmpty()) {
            i = free.remove(free.size());
            blocks.set(i, block);
        } else {
            i = blocks.size();
            blocks.add(i, block);
        }
        block.id(i);
        return i;
    }

    /**
     * Free a block, adding its index to the free list
     *
     * @param i the block index to free
     */
    @Override
    public void freeBlock(int i) {
        if (i < 0) {
            throw new IllegalArgumentException("i must be greater than zero");
        }
        blocks.set(i, null);
        free.add(i);
    }

    /**
     * Read a block
     *
     * @param blockId the index create the block to read
     * @return the block
     */
    @Override
    public Node<K, V> readBlock(int blockId) {
        if (blockId < 0) {
            throw new IllegalArgumentException("blockIndex must be greater than zero");
        }
        Node<K, V> found = blocks.get(blockId);
        if (found != null) {
            found.id(blockId);
        }
        return found;
    }

    /**
     * Write a node
     *
     * @param node    the node
     */
    @Override
    public void writeBlock(Node<K, V> node) {
        if (node.id() < 0) {
            throw new IllegalArgumentException("node index, must be greater than zero");
        }
        blocks.set(node.id(), node);
    }
}
