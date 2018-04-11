package io.joshworks.lsmtree;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

class LogEntry<K, V> {

    final K key;
    final V value;
    final long timestamp;

    private LogEntry(K key, V value, long timestamp) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }

    public static <K, V> Serializer<LogEntry<K, V>> serializer(final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
        return new EntrySerializer<>(keySerializer, valueSerializer);
    }

    public static <K, V> LogEntry<K, V> create(K key, V value) {
        return new LogEntry<>(key, value, System.currentTimeMillis());
    }

    public static <K, V> LogEntry<K, V> of(K key, V value, long timestamp) {
        return new LogEntry<>(key, value, timestamp);
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
            ByteBuffer valueDataBuffer = valueSerializer.toBytes(entry.value);

            ByteBuffer finalBuffer = ByteBuffer.allocate(keyDataBuffer.limit() + valueDataBuffer.limit() + Long.BYTES);
            finalBuffer.put(keyDataBuffer);
            finalBuffer.put(valueDataBuffer);
            finalBuffer.putLong(entry.timestamp);

            return (ByteBuffer) finalBuffer.flip();
        }

        @Override
        public LogEntry<K, V> fromBytes(ByteBuffer buffer) {
            K key = keySerializer.fromBytes(buffer);
            V value = valueSerializer.fromBytes(buffer);
            long timestamp = buffer.getLong();
            return new LogEntry<>(key, value, timestamp);
        }
    }

}
