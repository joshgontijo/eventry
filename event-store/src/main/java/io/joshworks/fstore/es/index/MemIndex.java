package io.joshworks.fstore.es.index;


import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.PollingSubscriber;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MemIndex implements Index {

    private final Map<Long, SortedSet<IndexEntry>> index = new ConcurrentHashMap<>();
    private final List<IndexEntry> insertOrder = new ArrayList<>();
    private final AtomicInteger size = new AtomicInteger();

    private final List<MemPoller> pollers = new ArrayList<>();

    public void add(IndexEntry entry) {
        index.compute(entry.stream, (k, v) -> {
            if (v == null)
                v = new TreeSet<>();
            v.add(entry);
            size.incrementAndGet();
            insertOrder.add(entry);
//            adToPollers(entry);
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
        for (MemPoller poller : new ArrayList<>(pollers)) {
            poller.close();
        }
    }

    @Override
    public LogIterator<IndexEntry> iterator(Direction direction) {
        List<IndexEntry> ordered = index.entrySet().stream()
                .sorted(Comparator.comparingLong(Map.Entry::getKey))
                .map(Map.Entry::getValue)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        return Iterators.of(ordered);
    }

    @Override
    public LogIterator<IndexEntry> iterator(Direction direction, Range range) {
        SortedSet<IndexEntry> entries = index.get(range.stream);
        if (entries == null || entries.isEmpty()) {
            return Iterators.empty();
        }
        Set<IndexEntry> indexEntries = entries.subSet(range.start(), range.end());
        if(Direction.FORWARD.equals(direction)) {
            return Iterators.of(Collections.unmodifiableSet(indexEntries));
        }
        return Iterators.reversed(List.copyOf(indexEntries));
    }

    @Override
    public Stream<IndexEntry> stream(Direction direction) {
        return Iterators.stream(iterator(direction));
    }

    @Override
    public Stream<IndexEntry> stream(Direction direction, Range range) {
        return Iterators.stream(iterator(direction, range));
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


//    private void adToPollers(IndexEntry entry) {
//        for (MemPoller poller : new ArrayList<>(pollers)) {
//            poller.add(entry);
//        }
//    }

    PollingSubscriber<IndexEntry> poller() {
        MemPoller memPoller = new MemPoller();
        pollers.add(memPoller);
        return memPoller;
    }

    private class MemPoller implements PollingSubscriber<IndexEntry> {

        private static final int VERIFICATION_INTERVAL_MILLIS = 500;
        private final AtomicBoolean closed = new AtomicBoolean();
        private int position = 0;

        private boolean hasData() {
            return position < insertOrder.size();
        }

        private void waitFor(long time, TimeUnit timeUnit) throws InterruptedException {
            if(time < 0) {
                return;
            }
            long elapsed = 0;
            long start = System.currentTimeMillis();
            long maxWaitTime = timeUnit.toMillis(time);
            long interval = Math.min(maxWaitTime, VERIFICATION_INTERVAL_MILLIS);
            while (!closed.get() && !hasData() && elapsed < maxWaitTime) {
                TimeUnit.MILLISECONDS.sleep(interval);
                elapsed = System.currentTimeMillis() - start;
            }
        }

        private void waitForData(long time, TimeUnit timeUnit) throws InterruptedException {
            while (!closed.get() && !hasData()) {
                timeUnit.sleep(time);
            }
        }

        @Override
        public synchronized IndexEntry peek() {
            if (hasData()) {
                return insertOrder.get(position);
            }
            return null;
        }

        @Override
        public synchronized IndexEntry poll() throws InterruptedException {
            return poll(-1, TimeUnit.SECONDS);
        }

        @Override
        public synchronized IndexEntry poll(long limit, TimeUnit timeUnit) throws InterruptedException {
            if (hasData()) {
                return insertOrder.get(position++);
            }
            waitFor(limit, timeUnit);
            if (hasData()) {
                return insertOrder.get(position++);
            }
            return null;
        }

        @Override
        public synchronized IndexEntry take() throws InterruptedException {
            if (hasData()) {
                return insertOrder.get(position++);
            }
            waitForData(VERIFICATION_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
            if (hasData()) {
                return insertOrder.get(position++);
            }
            //poller was closed while waiting for data
            return null; //TODO shouldn't be an InterruptedException ?
        }

        @Override
        public synchronized boolean headOfLog() {
            return !hasData();
        }

        @Override
        public synchronized boolean endOfLog() {
            return closed.get() && !hasData();
        }

        @Override
        public long position() {
            return position;
        }

        @Override
        public void close() {
            closed.set(true);
        }

    }

}