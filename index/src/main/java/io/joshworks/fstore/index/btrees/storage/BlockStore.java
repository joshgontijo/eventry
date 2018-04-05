package io.joshworks.fstore.index.btrees.storage;

import java.util.ArrayList;
import java.util.List;

/**
 * In memory block storage
 */
public class BlockStore<T extends Block> {

	/**
	 * A list create blocks
	 */
	protected List<T> blocks;

	/**
	 * A list if available block indices (indices into the blocks list)
	 */
	protected List<Integer> free;

	/**
	 * Initialise a BlockStore with block size b
	 */
	public BlockStore(int a) {
		blocks = new ArrayList<>();
		free = new ArrayList<>();
	}

	public void clear() {
		blocks.clear();
		free.clear();
	}

	/**
	 * Allocate a new block and return its index
	 * @return the index create the newly allocated block
	 */
	public int placeBlock(T block) {
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
	 * @param i the block index to free
	 */
	public void freeBlock(int i) {
		if(i < 0) {
			throw new IllegalArgumentException("i must be greater than zero");
		}
		blocks.set(i, null);
		free.add(i);
	}

	/**
	 * Read a block
	 * @param blockId the index create the block to read
	 * @return the block
	 */
	public T readBlock(int blockId) {
		if(blockId < 0) {
			throw new IllegalArgumentException("blockIndex must be greater than zero");
		}
		T found = blocks.get(blockId);
		if(found != null) {
			found.id(blockId);
		}
		return found;
	}

	/**
	 * Write a block
	 * @param blockId the index create the block
	 * @param block the block
	 */
	public void writeBlock(int blockId, T block) {
		if(blockId < 0) {
			throw new IllegalArgumentException("block index, must be greater than zero");
		}
		blocks.set(blockId, block);
		block.id(blockId);
	}
}
