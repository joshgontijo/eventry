package io.joshworks.fstore.index.hashmap;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class OffHeapHashMap<K, V> implements Map<K, V> {

    private static final double DEFAULT_LOAD_FACTOR = 0.75;

    private IntBuffer hashTable;
    private ByteBuffer data;

    private final Serializer<Node<K, V>> serializer;

    private final double loadFactor;
    private final int byteSize;
    private final int capacity;
    private int size;

    private OffHeapHashMap(double loadFactor, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        if (loadFactor < 0 || loadFactor > 1) {
            throw new IllegalArgumentException("Load factor must be between 0 and 1");
        }
        this.serializer = new EntrySerializer<>(keySerializer, valueSerializer);
        this.byteSize = 52428800;
        this.capacity = byteSize / Integer.BYTES;
        this.hashTable = ByteBuffer.allocateDirect(byteSize).asIntBuffer();
        this.data = ByteBuffer.allocateDirect(byteSize);
        this.loadFactor = loadFactor;
    }

    public static <K, V> Map<K, V> of(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        return of(keySerializer, valueSerializer, DEFAULT_LOAD_FACTOR);
    }

    public static <K, V> Map<K, V> of(Serializer<K> keySerializer, Serializer<V> valueSerializer, double loadFactor) {
        return new OffHeapHashMap<>(loadFactor, keySerializer, valueSerializer);
    }


    @Override
    public int size() {
        return size;

    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        Objects.requireNonNull(key, "Key must be provided");
        return false;
    }

    @Override
    public boolean containsValue(Object value) {
        return false;
    }

    @Override
    public V get(Object key) {
        return null;
    }

    @Override
    public V put(K key, V value) {
        Objects.requireNonNull(key, "Key must be provided");
        int hash = key.hashCode();

        int keyIdx = capacity / hash;

        int dataIdx = hashTable.get(keyIdx);
        if (dataIdx > 0) { //entry already exist. possible collision
            System.out.println("Already exist");

            //sync
            ByteBuffer readOnly = data.asReadOnlyBuffer();
            readOnly.position(dataIdx);

            Node<K, V> entry = serializer.fromBytes(readOnly);
            if(entry.getKey().equals(key)) { // duplicated replace
                //sync
                entry.setValue(value);
                ByteBuffer entryBuffer = serializer.toBytes(entry);
                data.position(dataIdx);
                data.put(entryBuffer);
            }

        }



        //sync
        ByteBuffer entryBuffer = serializer.toBytes(new Node<>(key, value));
        int position = data.position();
        data.put(entryBuffer);
        hashTable.put(keyIdx, position);

        return null;

    }

    @Override
    public V remove(Object key) {
        return null;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {

    }

    @Override
    public void clear() {

    }

    @Override
    public Set<K> keySet() {
        return null;
    }

    @Override
    public Collection<V> values() {
        return null;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return null;
    }
}
