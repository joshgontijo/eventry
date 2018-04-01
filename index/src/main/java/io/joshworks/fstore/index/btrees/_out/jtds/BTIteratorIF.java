package io.joshworks.fstore.index.btrees._out.jtds;



/**
 * Interface BTIteratorIF
 * @author tnguyen
 */
public interface BTIteratorIF <K extends Comparable, V> {
    public boolean item(K key, V value);
}
