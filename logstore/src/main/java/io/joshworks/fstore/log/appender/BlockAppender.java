package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.log.segment.block.Block;

import java.util.ArrayList;
import java.util.Iterator;

public class BlockAppender<T> extends SimpleLogAppender<Block<T>>{

    private final int blockSize;
    private final Serializer<T> serializer;

    private Block<T> block;

    public BlockAppender(Config<Block<T>> config, Serializer<T> serializer, int blockSize) {
        super(config);
        this.blockSize = blockSize;
        this.serializer = serializer;
        this.block = Block.newBlock(serializer, blockSize);
    }

    public boolean add(T data) {
        return block.add(data);
    }

    public long append(Block<T> block) {
        if (block.entryCount() <= 0) {
            throw new IllegalArgumentException("Block is empty");
        }
        return super.append(block);
    }

    @Override
    public void flush() {
        flushBlock();
        super.flush();
    }

    public long flushBlock() {
        if (block.entryCount() <= 0) {
            return -1;
        }
        long position = super.append(block);

        block = Block.newBlock(serializer, blockSize);
        return position;
    }

    public Iterator<T> iterator() {
        return new BlockItemIterator<>(scanner());
    }

    private static class BlockItemIterator<T> implements Iterator<T> {

        private Iterator<T> blockIterator;
        private final Iterator<Block<T>> appenderIterator;

        private BlockItemIterator(Iterator<Block<T>> iterator) {
            this.appenderIterator = iterator;
            blockIterator = appenderIterator.hasNext() ? appenderIterator.next().iterator() : new ArrayList<T>().iterator();
        }

        @Override
        public boolean hasNext() {
            if (blockIterator.hasNext()) {
                return true;
            }
            if (!appenderIterator.hasNext()) {
                return false;
            }
            blockIterator = appenderIterator.next().iterator();
            return blockIterator.hasNext();
        }

        @Override
        public T next() {
            return blockIterator.next();
        }
    }

}
