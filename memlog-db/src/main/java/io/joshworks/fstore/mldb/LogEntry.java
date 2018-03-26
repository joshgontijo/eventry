package io.joshworks.fstore.mldb;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class LogEntry<K, V> {

    public static final int OP_DELETE = 1;
    public static final int OP_ADD = 0;
    final int op;
    final K key;
    final V value;

    private LogEntry(int op, K key, V value) {
        this.op = op;
        this.key = key;
        this.value = value;
    }

    public static <K, V> LogEntry<K, V> delete(K key) {
        return new LogEntry<>(OP_DELETE, key, null);
    }

    public static <K, V> LogEntry<K, V> add(K key, V value) {
        return new LogEntry<>(OP_ADD, key, value);
    }

    public static <K, V> Serializer<LogEntry<K, V>> serializer(final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
        return new EntrySerializer<>(keySerializer, valueSerializer);
    }

    private static final class EntrySerializer<K, V> implements Serializer<LogEntry<K, V>> {

        private final Serializer<K> keySerializer;
        private final Serializer<V> valueSerializer;

        public EntrySerializer(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
        }

        @Override
        public ByteBuffer toBytes(LogEntry<K, V> entry) {
            ByteBuffer keyDataBuffer = keySerializer.toBytes(entry.key);
            if(entry.op == LogEntry.OP_DELETE) {
                ByteBuffer withOp = ByteBuffer.allocate(Integer.BYTES + keyDataBuffer.limit());
                return (ByteBuffer) withOp.putInt(entry.op).put(keyDataBuffer).flip();
            }
            ByteBuffer valueDataBuffer = valueSerializer.toBytes(entry.value);
            ByteBuffer withOp = ByteBuffer.allocate(Integer.BYTES + keyDataBuffer.limit() + valueDataBuffer.limit());
            return (ByteBuffer) withOp.putInt(entry.op).put(keyDataBuffer).put(valueDataBuffer).flip();
        }

        @Override
        public LogEntry<K, V> fromBytes(ByteBuffer buffer) {
            int op = buffer.getInt();
            if (op != OP_ADD && op != OP_DELETE) {
                throw new RuntimeException("Invalid OP");
            }
            K key = keySerializer.fromBytes(buffer);
            if(op == OP_DELETE) {
                return LogEntry.delete(key);
            }
            V value = valueSerializer.fromBytes(buffer);
            return LogEntry.add(key, value);
        }
    }

}
