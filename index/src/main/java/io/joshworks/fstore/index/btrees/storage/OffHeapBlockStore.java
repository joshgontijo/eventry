//package io.joshworks.fstore.index.btrees.storage;
//
//import io.joshworks.fstore.core.Serializer;
//import io.joshworks.fstore.index.btrees.bplustree.Node;
//
//import java.nio.ByteBuffer;
//import java.util.ArrayList;
//import java.util.List;
//
///**
// * In memory block storage
// */
//public class OffHeapBlockStore<K extends Comparable<K>, V> extends BlockStore<Node<K, V>> {
//
//    /**
//     * A list create blocks
//     */
//    private final ByteBuffer blocks;
//
//    private int blockCount;
//
//    /**
//     * A list if available block indices (indices into the blocks list)
//     */
//    protected List<Integer> free = new ArrayList<>();
//    private int blockSize;
//    private final Serializer<K> keySerializer;
//    private final Serializer<V> valueSerializer;
//
//    private final byte[] emptyBlock;
//
//    /**
//     * Initialise a BlockStore with block size b
//     *
//     * @param blockSize the block size
//     */
//    public OffHeapBlockStore(int blockSize, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
//        this.blockSize = blockSize;
//        this.keySerializer = keySerializer;
//        this.valueSerializer = valueSerializer;
//        this.blocks = ByteBuffer.allocateDirect(10485760);
//        this.emptyBlock = new byte[blockSize];
//    }
//
//    public void clear() {
//        blocks.clear();
//        free.clear();
//    }
//
//    /**
//     * Allocate a new block and return its index
//     *
//     * @return the index create the newly allocated block
//     */
//    public int placeBlock(Node<K, V> block) {
//        int i = free.isEmpty() ? free.remove(free.size()) : ++blockCount;
//        block.id(i);
//        ByteBuffer data = serializer.toBytes(block);
//        write(i, data);
//
//        return i;
//    }
//
//    /**
//     * Free a block, adding its index to the free list
//     *
//     * @param i the block index to free
//     */
//    public void freeBlock(int i) {
//        if (i < 0) {
//            throw new IllegalArgumentException("i must be greater than zero");
//        }
//        write(i, ByteBuffer.wrap(emptyBlock));
//        free.add(i);
//    }
//
//    /**
//     * Read a block
//     *
//     * @param blockId the index create the block to read
//     * @return the block
//     */
//    public Node<K, V> readBlock(int blockId) {
//        if (blockId < 0) {
//            throw new IllegalArgumentException("blockIndex must be greater than zero");
//        }
//        ByteBuffer read = read(blockId);
//        Node<K, V> node = serializer.fromBytes(read);
//
//        if (node == null) {
//            throw new IllegalStateException("Block with id " + blockId + " not found");
//        }
//        node.id(blockId);
//        return node;
//    }
//
//    private void write(int blockId, ByteBuffer data) {
//        blocks.mark();
//        blocks.position(blockId * blockSize);
//        blocks.put(data);
//        blocks.reset();
//    }
//
//    private ByteBuffer read(int blockId) {
//        blocks.mark();
//        blocks.position(blockId * blockSize);
//
//        byte[] block = new byte[blockSize];
//        blocks.get(block);
//
//        return ByteBuffer.wrap(block);
//
//    }
//
//    public void writeBlock(int blockId, Node<K, V> block) {
//        if (blockId < 0) {
//            throw new IllegalArgumentException("block index, must be greater than zero");
//        }
//
//        block.id(blockId);
//        ByteBuffer data = serializer.toBytes(block);
//        write(blockId, data);
//    }
//}
