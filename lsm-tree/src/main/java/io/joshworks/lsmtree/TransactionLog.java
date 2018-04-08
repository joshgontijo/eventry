package io.joshworks.lsmtree;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.log.appender.Builder;
import io.joshworks.fstore.log.appender.LogAppender;

import java.io.Closeable;
import java.io.File;
import java.util.Iterator;

public class TransactionLog<K, V> implements Closeable {

    private final File txDirectory;
    private final LogAppender<LogEntry<K, V>> appender;

    public TransactionLog(File directory, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.txDirectory = new File(directory, "tx");
        Serializer<LogEntry<K, V>> serializer = LogEntry.serializer(keySerializer, valueSerializer);
        this.appender = LogAppender.simpleLog(new Builder<>(txDirectory, serializer).asyncFlush());
    }

    public void put(K key, V data) {
        appender.append(LogEntry.add(key, data));
    }

    public void delete(K key) {
        appender.append(LogEntry.delete(key));
    }

    public Iterator<LogEntry<K, V>> iterator() {
        return appender.scanner();
    }

    public void clearLog() {
        appender.delete();
    }

    @Override
    public void close() {
        appender.close();
    }
}
