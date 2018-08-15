package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.DataReader;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.PollingSubscriber;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.Segment;
import io.joshworks.fstore.log.segment.SegmentState;
import io.joshworks.fstore.log.segment.Type;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public abstract class BlockSegment<T, B extends Block<T>> implements Log<T> {

    private B block;
    private final Segment<B> delegate;

    public BlockSegment(Storage storage, Serializer<B> serializer, DataReader reader, String magic, Type type) {
        delegate = new Segment<>(storage, serializer, reader, magic, type);
        this.block = createBlock();
    }

    protected abstract B createBlock();

    @Override
    public long append(T data) {
        if (block.add(data)) {
            return writeBlock();
        }
        return delegate.position();
    }

    protected long writeBlock() {
        if (block.isEmpty()) {
            return delegate.position();
        }
        long position = delegate.append(block);
        block = createBlock();
        return position;
    }

    protected B currentBlock() {
        return block;
    }

    @Override
    public void flush() {
        writeBlock();
        delegate.flush();
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public LogIterator<T> iterator() {
        return new BlockIterator<>(delegate.iterator());
    }

    @Override
    public Stream<T> stream() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator(), Spliterator.ORDERED), false);
    }

    @Override
    public LogIterator<T> iterator(long position) {
        return new BlockIterator<>(delegate.iterator(position));
    }

    @Override
    public long position() {
        return delegate.position();
    }

    @Override
    public T get(long position) {
        throw new UnsupportedOperationException("Block segment cannot get single item");
    }

    public B getBlock(long position) {
        B found = delegate.get(position);
        if (found == null) {
            throw new IllegalStateException("Block not found on address " + position);
        }
        return found;
    }

    @Override
    public long created() {
        return delegate.created();
    }

    @Override
    public long size() {
        return delegate.size();
    }

    @Override
    public SegmentState rebuildState(long lastKnownPosition) {
        return delegate.rebuildState(lastKnownPosition);
    }

    @Override
    public void delete() {
        block = createBlock();
        delegate.delete();
    }


    @Override
    public void roll(int level, ByteBuffer footer) {
        delegate.roll(level, footer);
    }

    @Override
    public ByteBuffer readFooter() {
        return delegate.readFooter();
    }

    @Override
    public PollingSubscriber<T> poller(long position) {
        return null;
    }

    @Override
    public PollingSubscriber<T> poller() {
        return null;
    }

    @Override
    public void roll(int level) {
        flush();
        delegate.roll(level);
    }

    @Override
    public boolean readOnly() {
        return delegate.readOnly();
    }

    @Override
    public long entries() {
        //TODO this will return the number of blocks
        return delegate.entries();
    }

    @Override
    public int level() {
        return delegate.level();
    }

    @Override
    public void close() {
        flush();
        delegate.close();
    }

    private static class BlockIterator<T, B extends Block<T>> implements LogIterator<T> {

        private final LogIterator<B> segmentIterator;
        private Queue<T> entries = new LinkedList<>();

        public BlockIterator(LogIterator<B> segmentIterator) {
            this.segmentIterator = segmentIterator;
        }

        @Override
        public long position() {
            return segmentIterator.position();
        }

        @Override
        public boolean hasNext() {
            if (!entries.isEmpty()) {
                return true;
            }
            if (!segmentIterator.hasNext()) {
                return false;
            }
            B block = segmentIterator.next();
            entries.addAll(block.entries());

            return !entries.isEmpty();
        }

        @Override
        public T next() {
            if (entries.isEmpty()) {
                throw new NoSuchElementException();
            }
            return entries.poll();
        }

        @Override
        public void close() {
            try {
                segmentIterator.close();
            } catch (IOException e) {
                throw RuntimeIOException.of(e);
            }
        }
    }
}
