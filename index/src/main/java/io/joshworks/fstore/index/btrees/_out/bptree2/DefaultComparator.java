/**
 * 
 */
package io.joshworks.fstore.index.btrees._out.bptree2;

import java.util.Comparator;

class DefaultComparator<T> implements Comparator<T> {
	@SuppressWarnings("unchecked")
	public int compare(T a, T b) {
		return ((Comparable<T>)a).compareTo(b);
	}
}