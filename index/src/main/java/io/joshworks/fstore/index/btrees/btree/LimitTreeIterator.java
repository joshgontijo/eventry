package io.joshworks.fstore.index.btrees.btree;

import io.joshworks.fstore.index.btrees.Entry;
import io.joshworks.fstore.index.btrees.storage.BlockStore;

import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class LimitTreeIterator<K extends Comparable<K>, V> extends TreeIterator<K, V> {

    private final int limit;
    private final AtomicInteger counter = new AtomicInteger(0);
    private final AtomicBoolean limitReached = new AtomicBoolean();

    LimitTreeIterator(int rootId, BlockStore<Node<K, V>> bs, int limit) {
        super(rootId, bs);
        this.limit = limit;
    }

    LimitTreeIterator(K x, int rootId, BlockStore<Node<K, V>> bs, int limit) {
        super(x, null, rootId, bs);
        this.limit = limit;
    }

    @Override
    public boolean hasNext() {
        return counter.get() < limit && super.hasNext();
    }

    @Override
    public Entry<K, V> next() {
        if(limitReached.compareAndSet(true, counter.incrementAndGet() < limit)) {
            throw new NoSuchElementException();
        }
        return super.next();
    }
}
