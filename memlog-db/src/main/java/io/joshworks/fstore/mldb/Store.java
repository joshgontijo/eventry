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

public class Store<K extends Comparable<K>, V> implements Closeable {

    private final Index<K, Long> index;
    private final LogAppender<LogEntry<K, V>> log;

    private Store(Index<K, Long> index, LogAppender<LogEntry<K, V>> log) {
        this.index = index;
        this.log = log;
    }

    public static <K extends Comparable<K>, V> Store<K, V> open(File directory, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        Store<K, V> kvStore = new Store<>(new MemIndex<>(), LogAppender.simpleLog(new Builder<>(directory, LogEntry.serializer(keySerializer, valueSerializer))));
        kvStore.reindex();
        return kvStore;
    }

    public V get(K key) {
        Long address = index.get(key);
        if (address == null) {
            return null;
        }
        return log.get(address).value;
    }

    public void put(K key, V value) {
        if (key == null) {
            throw new IllegalArgumentException("Key must be provided");
        }
        long address = log.append(LogEntry.add(key, value));
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
        V data = log.get(address).value;
        if (data != null) {
            log.append(LogEntry.delete(key));
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
        Scanner<LogEntry<K, V>> scanner = log.scanner();

        long position = scanner.position();
        for (LogEntry<K, V> entry : scanner) {
            System.out.println("KEY: " + entry.key + " (" + position + ") --> " + entry.value + " -> OP: " + entry.op);
            if (entry.op == LogEntry.OP_DELETE)
                index.delete(entry.key);
            else
                index.put(entry.key, position);

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
