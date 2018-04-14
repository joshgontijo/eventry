package io.joshworks.fstore.index.bplustree.storage;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.index.bplustree.Node;
import io.joshworks.fstore.index.bplustree.NodeSerializer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * In memory block storage
 */
public class OffHeapBlockStore<K extends Comparable<K>, V> implements BlockStore<K, V> {

    /**
     * A list create blocks
     */
    private final ByteBuffer blocks;
    private final Serializer<Node<K, V>> serializer;

    private int blockCount;

    /**
     * A list if available block indices (indices into the blocks list)
     */
    protected List<Integer> free = new ArrayList<>();
    private final int blockSize;
    private final int order;

    private final byte[] emptyBlock;

    /**
     * Initialise a BlockStore with block size b
     *
     * @param blockSize the block size
     */
    //TODO on open should read tree info
    public OffHeapBlockStore(int blockSize, int order, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.blockSize = blockSize;
        this.order = order;
        this.serializer = new NodeSerializer<>(blockSize, order, this, keySerializer, valueSerializer);
        this.blocks = ByteBuffer.allocateDirect(10485760);
        this.emptyBlock = new byte[blockSize];
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
        int i = free.isEmpty() ? blockCount++ : free.remove(free.size() - 1);
        block.id(i);
        ByteBuffer data = serializer.toBytes(block);
        write(i, data);

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
        write(i, ByteBuffer.wrap(emptyBlock));
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
        ByteBuffer read = read(blockId);
        Node<K, V> node = serializer.fromBytes(read);

        if (node == null) {
            throw new IllegalStateException("Block with id " + blockId + " not found");
        }
        return node;
    }

    private void write(int blockId, ByteBuffer data) {
        synchronized (blocks) {
            blocks.position(blockId * blockSize);
            blocks.put(data);
        }
    }

    private ByteBuffer read(int blockId) {
        ByteBuffer readOnly = blocks.asReadOnlyBuffer();
        readOnly.position(blockId * blockSize);
        readOnly.limit(readOnly.position() + blockSize);

        byte[] block = new byte[blockSize];
        readOnly.get(block);

        return ByteBuffer.wrap(block);

    }

    @Override
    public void writeBlock(Node<K, V> block) {
        if (block.id() < 0) {
            throw new IllegalArgumentException("block index, must be greater than zero");
        }

        ByteBuffer data = serializer.toBytes(block);
        write(block.id(), data);
    }
}
