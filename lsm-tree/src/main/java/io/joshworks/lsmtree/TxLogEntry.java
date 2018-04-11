package io.joshworks.lsmtree;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

class TxLogEntry<K, V> {

    static final int OP_DELETE = 1;
    static final int OP_ADD = 0;

    final int op;
    final long timestamp;
    final K key;
    final V value;

    private TxLogEntry(int op, long timestamp, K key, V value) {
        this.op = op;
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
    }

    public static <K, V> TxLogEntry<K, V> delete(long timestamp, K key) {
        return new TxLogEntry<>(OP_DELETE, timestamp, key, null);
    }

    public static <K, V> TxLogEntry<K, V> add(long timestamp, K key, V value) {
        return new TxLogEntry<>(OP_ADD, timestamp, key, value);
    }

    public static <K, V> Serializer<TxLogEntry<K, V>> serializer(final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
        return new EntrySerializer<>(keySerializer, valueSerializer);
    }

    private static final class EntrySerializer<K, V> implements Serializer<TxLogEntry<K, V>> {

        private final Serializer<K> keySerializer;
        private final Serializer<V> valueSerializer;

        public EntrySerializer(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
        }

        @Override
        public ByteBuffer toBytes(TxLogEntry<K, V> entry) {
            ByteBuffer keyDataBuffer = keySerializer.toBytes(entry.key);
            if(entry.op == TxLogEntry.OP_DELETE) {
                ByteBuffer withOp = ByteBuffer.allocate(Integer.BYTES + keyDataBuffer.limit());
                return (ByteBuffer) withOp.putInt(entry.op).put(keyDataBuffer).flip();
            }
            ByteBuffer valueDataBuffer = valueSerializer.toBytes(entry.value);
            ByteBuffer withOp = ByteBuffer.allocate(Integer.BYTES + Long.BYTES + keyDataBuffer.limit() + valueDataBuffer.limit());
            return (ByteBuffer) withOp.putInt(entry.op).put(keyDataBuffer).put(valueDataBuffer).flip();
        }

        @Override
        public TxLogEntry<K, V> fromBytes(ByteBuffer buffer) {
            int op = buffer.getInt();
            if (op != OP_ADD && op != OP_DELETE) {
                throw new RuntimeException("Invalid OP");
            }
            long timestamp = buffer.getLong();
            K key = keySerializer.fromBytes(buffer);
            if(op == OP_DELETE) {
                return new TxLogEntry<>(OP_DELETE, timestamp, key, null);
            }
            V value = valueSerializer.fromBytes(buffer);
            return new TxLogEntry<>(OP_DELETE, timestamp, key, value);
        }
    }

}
