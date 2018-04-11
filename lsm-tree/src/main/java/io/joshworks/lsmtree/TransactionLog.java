package io.joshworks.lsmtree;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.log.appender.Builder;
import io.joshworks.fstore.log.appender.LogAppender;

import java.io.Closeable;
import java.io.File;
import java.util.Iterator;

public class TransactionLog<K, V> implements Closeable {

    private static final String LOCATION = "tx";
    private final File txDirectory;
    private final LogAppender<TxLogEntry<K, V>> appender;

    public TransactionLog(File directory, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.txDirectory = new File(directory, LOCATION);
        Serializer<TxLogEntry<K, V>> serializer = TxLogEntry.serializer(keySerializer, valueSerializer);
        this.appender = LogAppender.simpleLog(new Builder<>(txDirectory, serializer).asyncFlush());
    }

    public void put(LogEntry<K, V> entry) {
        appender.append(TxLogEntry.add(entry.timestamp, entry.key, entry.value));
    }

    public void delete(LogEntry<K, V> entry) {
        appender.append(TxLogEntry.delete(entry.timestamp, entry.key));
    }

    public Iterator<TxLogEntry<K, V>> iterator() {
        return appender.scanner();
    }

    //mark a rolling to
    public void mark() {
        appender.roll();
    }

    public long size() {
        return appender.size();
    }

    public void clearLog() {
        appender.position();
        appender.clear();
    }

    @Override
    public void close() {
        appender.close();
    }
}
