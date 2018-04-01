package io.joshworks.fstore.index;

import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class HeapTreeIndex<K extends Comparable<K>, V> implements SortedIndex<K, V> {

    private final SortedMap<K, V> index = new TreeMap<>();

    @Override
    public V get(K key) {
        nonNull(key);
        return index.get(key);
    }

    @Override
    public V put(K key, V value) {
        nonNull(key);
        return index.put(key, value);
    }

    @Override
    public V delete(K key) {
        nonNull(key);
        return index.remove(key);
    }

    @Override
    public void clear() {
        index.clear();
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        return iterator(new Range<>());
    }

    @Override
    public Iterator<Entry<K, V>> iterator(Range<K> range) {

        final SortedMap<K, V> subMap;
        if (range.startInclusive != null && range.endExclusive != null)
            subMap = index.subMap(range.startInclusive, range.endExclusive);
        else if (range.startInclusive != null)
            subMap = index.tailMap(range.startInclusive);
        else if (range.endExclusive != null)
            subMap = index.headMap(range.endExclusive);
        else
            subMap = index;

        Iterator<Map.Entry<K, V>> iterator = subMap.entrySet().iterator();
        for (int i = 0; i < range.skip; i++) {
            if(iterator.hasNext()) {
                iterator.next();
            }
        }

        final long limit = range.limit;
        return new Iterator<Entry<K, V>>() {

            private long count;

            @Override
            public boolean hasNext() {
                return iterator.hasNext() && (limit == Range.NO_LIMIT || count <= limit);
            }

            @Override
            public Entry<K, V> next() {
                Map.Entry<K, V> next = iterator.next();
                count++;
                return Entry.of(next.getKey(), next.getValue());
            }
        };
    }

    private void nonNull(K key) {
        if (key == null) {
            throw new IllegalArgumentException("Key must be provided");
        }
    }
}
