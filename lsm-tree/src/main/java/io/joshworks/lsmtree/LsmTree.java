package io.joshworks.lsmtree;

import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.lsmtree.block.BlockAppender;

import java.io.File;
import java.util.Iterator;
import java.util.Map;

public class LsmTree<K extends Comparable<K>, V> {

    private MemTable<K, LogEntry<K, V>> memtable = new MemTable<>();
    private final TransactionLog<K, V> wal;
    private final BlockAppender<LogEntry<K, V>> appender;

    public LsmTree(File directory, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.wal = new TransactionLog<>(directory, keySerializer, valueSerializer);
        this.appender = new BlockAppender<>(directory, new SnappyCodec());
        index();
    }

    public void add(K key, V value) {
        LogEntry<K, V> entry = LogEntry.create(key, value);
        wal.put(entry);
        memtable.put(key, entry);

        if(wal.size() > 5242880) { //5MB
            Iterator<Map.Entry<K, LogEntry<K, V>>> iterator = memtable.iterator();
            while (iterator.hasNext()) {
                Map.Entry<K, LogEntry<K, V>> next = iterator.next();
                appender.add(next.getValue());
            }
        }

    }

    public V get(K key) {
        LogEntry<K, V> found = memtable.get(key);
        if (found != null) {
            return found.value;
        }
        //TODO search files
        throw new UnsupportedOperationException("TODO");
    }

    public V delete(K key) {
        LogEntry<K, V> found = memtable.remove(key);
        if (found != null) {
            return found.value;
        }
        //TODO search files
        throw new UnsupportedOperationException("TODO");
    }

    private void index() {
        Iterator<TxLogEntry<K, V>> iterator = wal.iterator();
        while (iterator.hasNext()) {
            TxLogEntry<K, V> entry = iterator.next();
            if (entry.op == TxLogEntry.OP_ADD) {
                memtable.put(entry.key, LogEntry.of(entry.key, entry.value, entry.timestamp));
            } else {
                memtable.remove(entry.key);
            }
        }
    }

}
