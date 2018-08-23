package io.joshworks.fstore.es.index;


import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.PollingSubscriber;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class MemIndex implements Index {

    private final Map<Long, SortedSet<IndexEntry>> index = new ConcurrentHashMap<>();
    private final AtomicInteger size = new AtomicInteger();

    private final List<MemPoller> pollers = new ArrayList<>();

    public void add(IndexEntry entry) {
        index.compute(entry.stream, (k, v) -> {
            if (v == null)
                v = new TreeSet<>();
            v.add(entry);
            size.incrementAndGet();
            adToPollers(entry);
            return v;
        });
    }

    @Override
    public int version(long stream) {
        SortedSet<IndexEntry> entries = index.get(stream);
        if (entries == null) {
            return IndexEntry.NO_VERSION;
        }

        return entries.last().version;
    }

    public int size() {
        return size.get();
    }


    public boolean isEmpty() {
        return size.get() == 0;
    }

    @Override
    public void close() {
        index.clear();
        size.set(0);
        pollers.clear();
    }

    @Override
    public LogIterator<IndexEntry> iterator(Range range) {
        SortedSet<IndexEntry> entries = index.get(range.stream);
        if (entries == null) {
            return Iterators.empty();
        }
        Set<IndexEntry> indexEntries = Collections.unmodifiableSet(entries.subSet(range.start(), range.end()));
        return Iterators.of(indexEntries);
    }

    @Override
    public Stream<IndexEntry> stream() {
        return Iterators.stream(iterator());
    }

    @Override
    public Stream<IndexEntry> stream(Range range) {
        return Iterators.stream(iterator(range));
    }

    @Override
    public Optional<IndexEntry> get(long stream, int version) {
        SortedSet<IndexEntry> entries = index.get(stream);
        if (entries == null) {
            return Optional.empty();
        }

        Range range = Range.of(stream, version, version + 1);
        entries = entries.subSet(range.start(), range.end());
        if (entries.isEmpty()) {
            return Optional.empty();
        }
        if (entries.size() > 1) {
            throw new IllegalStateException("Found more than one index entry for stream " + stream + ", version " + version);
        }
        return Optional.of(entries.first());
    }

    @Override
    public LogIterator<IndexEntry> iterator() {
        return Iterators.of(Collections.unmodifiableSet(indexEntries()));
    }

    public SortedSet<IndexEntry> indexEntries() {
        return index.values().stream().reduce(new TreeSet<>(), (state, next) -> {
            state.addAll(next);
            return state;
        });
    }

    private void adToPollers(IndexEntry entry) {
        for (MemPoller poller : pollers) {
            poller.add(entry);
        }
    }

    PollingSubscriber<IndexEntry> poller() {
        MemPoller memPoller = new MemPoller(new LinkedBlockingDeque<>(indexEntries()));
        pollers.add(memPoller);
        return memPoller;
    }

    private class MemPoller implements PollingSubscriber<IndexEntry> {

        private final BlockingQueue<IndexEntry> queue;
        private final AtomicBoolean closed = new AtomicBoolean();

        private MemPoller(BlockingQueue<IndexEntry> queue) {
            this.queue = queue;
        }

        @Override
        public IndexEntry peek() {
            return queue.peek();
        }

        @Override
        public IndexEntry poll() {
            return queue.poll();
        }

        @Override
        public IndexEntry poll(long limit, TimeUnit timeUnit) throws InterruptedException {
            return queue.poll(limit, timeUnit);
        }

        @Override
        public IndexEntry take() throws InterruptedException {
            return queue.take();
        }

        @Override
        public boolean headOfLog() {
            return closed.get() && queue.isEmpty();
        }

        @Override
        public long position() {
            return -1;
        }

        @Override
        public void close() {
            closed.compareAndSet(false, true);
        }

        private void add(IndexEntry entry) {
            if (closed.get()) {
                return;
            }
            queue.add(entry);
        }
    }

}