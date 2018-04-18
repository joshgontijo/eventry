package io.joshworks.fstore.index.hashmap;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

class EntrySerializer<K, V> implements Serializer<Node<K, V>> {

    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;

    EntrySerializer(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;

    }

    @Override
    public ByteBuffer toBytes(Node<K, V> data) {

        ByteBuffer key = keySerializer.toBytes(data.getKey());
        ByteBuffer value = valueSerializer.toBytes(data.getValue());

        ByteBuffer bb = ByteBuffer.allocate(Integer.BYTES + key.limit() + key.limit());

        bb.putInt(data.getNext());
        bb.put(key);
        bb.put(value);

        return (ByteBuffer) bb.flip();
    }

    @Override
    public void writeTo(Node<K, V> data, ByteBuffer dest) {
        ByteBuffer key = keySerializer.toBytes(data.getKey());
        ByteBuffer value = valueSerializer.toBytes(data.getValue());

        dest.putInt(data.getNext());
        dest.put(key);
        dest.put(value);

    }

    @Override
    public Node<K, V> fromBytes(ByteBuffer buffer) {

        int next = buffer.getInt();

        K key = keySerializer.fromBytes(buffer);
        V value = valueSerializer.fromBytes(buffer);

        return new Node<>(key, value, next);


    }
}
