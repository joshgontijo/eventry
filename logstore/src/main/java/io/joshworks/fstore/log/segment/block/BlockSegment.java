package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.DataReader;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.PollingSubscriber;
import io.joshworks.fstore.log.TimeoutReader;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.Marker;
import io.joshworks.fstore.log.segment.Segment;
import io.joshworks.fstore.log.segment.SegmentState;
import io.joshworks.fstore.log.segment.Type;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public abstract class BlockSegment<T, B extends Block<T>> implements Log<T> {

    private final int maxBlockSize;
    private final Segment<B> delegate;
    private final Serializer<T> serializer;
    private B block;

    public BlockSegment(Storage storage, Serializer<T> serializer, Serializer<B> blockSerializer, int maxBlockSize, DataReader reader, String magic, Type type) {
        delegate = new Segment<>(storage, blockSerializer, reader, magic, type);
        this.serializer = serializer;
        this.maxBlockSize = maxBlockSize;
        this.block = createBlock(serializer, maxBlockSize);
    }

    protected abstract B createBlock(Serializer<T> serializer, int maxBlockSize);

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
        block = createBlock(serializer, maxBlockSize);
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
    public Stream<T> stream(Direction direction) {
        return Iterators.stream(iterator(direction));
    }

    @Override
    public Marker marker() {
        return delegate.marker();
    }

    @Override
    public Set<TimeoutReader> readers() {
        return delegate.readers();
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
        block = createBlock(serializer, maxBlockSize);
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
        return new BlockPoller(delegate.poller(position));
    }

    @Override
    public PollingSubscriber<T> poller() {
        return new BlockPoller(delegate.poller());
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

    @Override
    public LogIterator<T> iterator(Direction direction) {
        return new BlockIterator<>(delegate.iterator(direction), direction);
    }

    @Override
    public LogIterator<T> iterator(long position, Direction direction) {
        return new BlockIterator<>(delegate.iterator(position, direction), direction);
    }

    @Override
    public String toString() {
        return delegate.toString();
    }

    private static class BlockIterator<T, B extends Block<T>> implements LogIterator<T> {

        private final Direction direction;
        private final LogIterator<B> segmentIterator;
        private Queue<T> entries = new LinkedList<>();

        public BlockIterator(LogIterator<B> segmentIterator,  Direction direction) {
            this.segmentIterator = segmentIterator;
            this.direction = direction;
        }

        private List<T> readBlockEntries(B block) {
            List<T> blockEntries = block.entries();
            if(Direction.BACKWARD.equals(direction)) {
                Collections.reverse(blockEntries);
            }
            return blockEntries;
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
                IOUtils.closeQuietly(segmentIterator);
                return false;
            }
            B block = segmentIterator.next();
            entries.addAll(readBlockEntries(block));

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
            IOUtils.closeQuietly(segmentIterator);
        }
    }

    private final class BlockPoller implements PollingSubscriber<T> {

        private final PollingSubscriber<B> segmentPoller;
        private Queue<T> entries = new LinkedList<>();

        public BlockPoller(PollingSubscriber<B> segmentPoller) {
            this.segmentPoller = segmentPoller;
        }

        @Override
        public synchronized T peek() throws InterruptedException {
            if(entries.isEmpty()) {
                B polled = segmentPoller.poll();
                if(polled != null) {
                    entries.addAll(polled.entries());
                }
            }
            return entries.peek();
        }

        @Override
        public synchronized T poll() throws InterruptedException {
            if(entries.isEmpty()) {
                B polled = segmentPoller.poll();
                if(polled != null) {
                    entries.addAll(polled.entries());
                }
            }
            return entries.poll();
        }

        @Override
        public synchronized T poll(long limit, TimeUnit timeUnit) throws InterruptedException {
            if(entries.isEmpty()) {
                B polled = segmentPoller.poll(limit, timeUnit);
                if(polled != null) {
                    entries.addAll(polled.entries());
                }
            }
            return entries.poll();
        }

        @Override
        public synchronized T take() throws InterruptedException {
            if(entries.isEmpty()) {
                B polled = segmentPoller.take();
                if(polled != null) {
                    entries.addAll(polled.entries());
                }
            }
            return entries.poll();
        }

        @Override
        public synchronized boolean headOfLog() {
            return entries.isEmpty() && segmentPoller.headOfLog();
        }

        @Override
        public synchronized boolean endOfLog() {
            return entries.isEmpty() && segmentPoller.endOfLog();
        }

        @Override
        public long position() {
            return 0;
        }

        @Override
        public synchronized void close() throws IOException {
            segmentPoller.close();
        }
    }

}
