package io.joshworks.fstore.index;

import java.util.Iterator;

public interface SortedIndex<K extends Comparable<K>, V> extends Index<K, V>, Iterable<Entry<K, V>> {

    Iterator<Entry<K, V>> iterator();

    Iterator<Entry<K, V>> iterator(Range<K> range);
}
