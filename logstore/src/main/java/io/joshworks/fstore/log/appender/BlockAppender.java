package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.log.segment.LogSegment;
import io.joshworks.fstore.log.segment.block.Block;

import java.util.ArrayList;
import java.util.Iterator;

public class BlockAppender<T> {

    private final int blockSize;
    private final LogAppender<Block<T>, LogSegment<Block<T>>> appender;
    private final Serializer<T> serializer;

    private Block<T> block;

    public BlockAppender(LogAppender<Block<T>, LogSegment<Block<T>>> appender, Serializer<T> serializer, int blockSize) {
        this.blockSize = blockSize;
        this.serializer = serializer;
        this.appender = appender;

        this.block = Block.newBlock(serializer, blockSize);
    }

    public boolean append(T data) {
        return block.add(data);
    }

    public long add(Block<T> block){
        if (block.entryCount() <= 0) {
            throw new IllegalArgumentException("Block is empty");
        }
        return appender.append(block);
    }

    public long flush() {
        if (block.entryCount() <= 0) {
            return -1;
        }
        long position = appender.append(block);

        block = Block.newBlock(serializer, blockSize);
        return position;
    }

    public Iterator<T> iterator() {
        return new BlockItemIterator<>(appender);
    }

    private static class BlockItemIterator<T> implements Iterator<T> {

        private Iterator<T> blockIterator;
        private final Iterator<Block<T>> appenderIterator;

        private BlockItemIterator(LogAppender<Block<T>, LogSegment<Block<T>>> appender) {
            this.appenderIterator = appender.scanner();
            blockIterator = appenderIterator.hasNext() ? appenderIterator.next().iterator() : new ArrayList<T>().iterator();
        }

        @Override
        public boolean hasNext() {
            if(blockIterator.hasNext()) {
                return true;
            }
            if(!appenderIterator.hasNext()) {
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
