package io.joshworks.fstore.index.btrees.btree;

public class Search {

    /**
     * Find the index i, using a binary search, at which x should be inserted into the null-padded
     * sorted array, a
     *
     * @param node the sorted array (padded with null entries)
     * @param x    the value to search for
     * @return i or -i-1 if a[i] equals x
     */
    static <K extends Comparable<K>, V> int binarySearch(Node<K, V> node, K x) {
        int lo = 0, hi = node.n;
        while (hi != lo) {
            int m = (hi + lo) / 2;
            int cmp = node.entries[m] == null ? -1 : x.compareTo(node.entries[m].key);
            if (cmp < 0)
                hi = m;      // look in first half
            else if (cmp > 0)
                lo = m + 1;    // look in second half
            else
                return -m - 1; // found it
        }
        return lo;
    }

}
