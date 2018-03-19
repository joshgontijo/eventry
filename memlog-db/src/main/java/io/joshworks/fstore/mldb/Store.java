package io.joshworks.fstore.mldb;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.index.Index;
import io.joshworks.fstore.index.MemIndex;
import io.joshworks.fstore.log.Scanner;
import io.joshworks.fstore.log.appender.Builder;
import io.joshworks.fstore.log.appender.LogAppender;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

public class Store<K extends Comparable<K>, V> implements Closeable {

    private final Index<K, Long> index;
    private final LogAppender<LogEntry<V>> log;
    private final Function<V, K> keyMapper;

    private Store(Index<K, Long> index, LogAppender<LogEntry<V>> log, Function<V, K> keyMapper) {
        this.index = index;
        this.log = log;
        this.keyMapper = keyMapper;
    }

    public static <K extends Comparable<K>, V> Store<K, V> create(File directory, Serializer<V> serializer, Function<V, K> keyMapper) {
        return new Store<>(new MemIndex<>(), LogAppender.create(new Builder<>(directory, LogEntry.serializer(serializer))), keyMapper);
    }

    public static <K extends Comparable<K>, V> Store<K, V> open(File directory, Serializer<V> serializer, Function<V, K> keyMapper) {
        Store<K, V> kvStore = new Store<>(new MemIndex<>(), LogAppender.open(directory, LogEntry.serializer(serializer)), keyMapper);
        kvStore.reindex();
        return kvStore;
    }

    public V get(K key) {
        Long address = index.get(key);
        if (address == null) {
            return null;
        }
        return log.get(address).data;
    }

    public void put(V value) {
        K key = keyMapper.apply(value);
        if (key == null) {
            throw new IllegalArgumentException("Key must be provided");
        }
        long address = log.append(LogEntry.of(0, value));
        Long old = index.put(key, address);
        if (old != null) {
            System.out.println("Overriding " + old);
        }
    }

    public V delete(K key) {
        Long address = index.delete(key);
        if (address == null) {
            return null;
        }
        V data = log.get(address).data;
        if (data != null) {
            log.append(LogEntry.of(1, data));
        }

        return data;
    }


    public Iterator<V> iterator() {
        return new Iterator<V>() {
            private Iterator<Map.Entry<K, Long>> indexIterator = index.iterator();

            @Override
            public boolean hasNext() {
                return indexIterator.hasNext();
            }

            @Override
            public V next() {
                Map.Entry<K, Long> next = indexIterator.next();
                return get(next.getKey());
            }
        };
    }

    private void reindex() {
        long start = System.currentTimeMillis();
        System.out.println("Reindexing...");
        Scanner<LogEntry<V>> scanner = log.scanner();

        long position = scanner.position();
        for (LogEntry<V> value : scanner) {
            System.out.println("POS: " + position + " --> " + value.data + " -> OP: " + value.op);
            K k = keyMapper.apply(value.data);
            if (value.op == 1)
                index.delete(k);
            else
                index.put(k, position);

            position = scanner.position();
        }
        System.out.println("Reindexing complete in " + (System.currentTimeMillis() - start) + "ms");
    }

    @Override
    public void close() throws IOException {
        index.clear();
        log.close();
    }
}
