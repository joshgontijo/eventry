package io.joshworks.fstore.index.btrees;

public class Entry<K extends Comparable<K>, V> implements Comparable<Entry<K, V>> {
    public final K key;
    public final V value;

    private Entry(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public static <K extends Comparable<K>, V> Entry<K, V> of(K k, V v) {
        return new Entry<>(k, v);
    }

    public static <K extends Comparable<K>, V> Entry<K, V> empty() {
        return new Entry<>(null, null);
    }

    @Override
    public int compareTo(Entry<K, V> o) {
        return key.compareTo(o.key);
    }

    @Override
    public String toString() {
        return "[" + String.valueOf(key) + "=" + String.valueOf(value) + "]";
    }
}
