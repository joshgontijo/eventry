package io.joshworks.fstore.index.bplustree.storage;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.index.bplustree.BPlusTree;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * In memory block storage
 */
public class OffHeapBlockStore implements BlockStore {

    /**
     * A list create blocks
     */
    private final ByteBuffer blocks;
    private final Serializer<BPlusTree.Node> serializer;

    private int blockCount;

    /**
     * A list if available block indices (indices into the blocks list)
     */
    protected List<Integer> free = new ArrayList<>();
    private int blockSize;

    private final byte[] emptyBlock;

    /**
     * Initialise a BlockStore with block size b
     *
     * @param blockSize the block size
     */
    public OffHeapBlockStore(int blockSize, Serializer<BPlusTree.Node> serializer) {
        this.blockSize = blockSize;
        this.serializer = serializer;
        this.blocks = ByteBuffer.allocateDirect(10485760);
        this.emptyBlock = new byte[blockSize];
    }

    public void clear() {
        blocks.clear();
        free.clear();
    }

    /**
     * Allocate a new block and return its index
     *
     * @return the index create the newly allocated block
     */
    public int placeBlock(BPlusTree.Node block) {
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
    public BPlusTree.Node readBlock(int blockId) {
        if (blockId < 0) {
            throw new IllegalArgumentException("blockIndex must be greater than zero");
        }
        ByteBuffer read = read(blockId);
        BPlusTree.Node node = serializer.fromBytes(read);

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

    public void writeBlock(int blockId, BPlusTree.Node block) {
        if (blockId < 0) {
            throw new IllegalArgumentException("block index, must be greater than zero");
        }

        block.id(blockId);
        ByteBuffer data = serializer.toBytes(block);
        write(blockId, data);
    }
}
