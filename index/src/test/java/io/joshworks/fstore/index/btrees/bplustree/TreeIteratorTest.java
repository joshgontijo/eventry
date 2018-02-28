package io.joshworks.fstore.index.btrees.bplustree;

import io.joshworks.fstore.index.btrees.Entry;
import io.joshworks.fstore.index.btrees.storage.BlockStore;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;


public class TreeIteratorTest {

    private BlockStore<Node<Integer, String>> store;

    @Before
    public void setUp() {
        store = new BlockStore<>(0);
    }

    @Test
    public void iterator() {
        BPlusTree<Integer, String> tree = BPlusTree.of(store, 3);

        int size = 100;
        int startKey = 0;
        int endKey = 0;
        for (int i = 0; i < size; i++) {
            tree.put(i, String.valueOf(i));
        }

        assertIterator(tree.iterator(), size, 0, 499);
    }

    @Test
    public void range() {
        BPlusTree<Integer, String> tree = BPlusTree.of(store, 3);

        for (int i = 0; i < 400; i++) {
            tree.put(i, String.valueOf(i));
        }

        int startKey = 100;
        int endKey = 400;
        int expectedSize = endKey - startKey;
        int expectedLastKey = endKey - 1;
        assertIterator(tree.range(100, 400), expectedSize, startKey, expectedLastKey);
    }

    @Test
    public void range_out_of_range() {
        BPlusTree<Integer, String> tree = BPlusTree.of(store, 3);

        int size = 102;
        for (int i = 0; i < size; i++) {
            tree.put(i, String.valueOf(i));
        }

        int startKey = 100;
        int expectedSize = 2;
        int expectedLastKey = 101;
        assertIterator(tree.range(100, 400), expectedSize, startKey, expectedLastKey);
    }

    @Test
    public void limit_skip() {
        BPlusTree<Integer, String> tree = BPlusTree.of(store, 3);

        int size = 500;
        for (int i = 0; i < size; i++) {
            tree.put(i, String.valueOf(i));
        }

        int startKey = 100;
        int expectedSize = 400;
        int expectedLastKey = 500;
        assertIterator(tree.limit(100, 999), expectedSize, startKey, expectedLastKey);

    }

    @Test
    public void limit_limit() {
        BPlusTree<Integer, String> tree = BPlusTree.of(store, 3);

        int size = 500;
        for (int i = 0; i < size; i++) {
            tree.put(i, String.valueOf(i));
        }

        int startKey = 0;
        int expectedSize = 10;
        int expectedLastKey = 10;
        assertIterator(tree.limit(0, 10), expectedSize, startKey, expectedLastKey);
    }

    @Test
    public void startInclusive_limit() {
        BPlusTree<Integer, String> tree = BPlusTree.of(store, 3);

        int size = 500;
        for (int i = 0; i < size; i++) {
            tree.put(i, String.valueOf(i));
        }

        int startKey = 450;
        int expectedSize = 10;
        int expectedLastKey = 459;
        assertIterator(tree.limit(0, 10, 450), expectedSize, startKey, expectedLastKey);

    }

    @Test
    public void startInclusive_limit_skip() {
        BPlusTree<Integer, String> tree = BPlusTree.of(store, 3);

        int size = 500;
        for (int i = 0; i < size; i++) {
            tree.put(i, String.valueOf(i));
        }

        int startKey = 455;
        int expectedSize = 10;
        int expectedLastKey = 464;
        assertIterator(tree.limit(5, 10, 450), expectedSize, startKey, expectedLastKey);

    }

    private void assertIterator(Iterator<Entry<Integer, String>> iterator, int expectedSize, int expectedFirstKey, int expectedLastKey) {
        long last = expectedFirstKey - 1;
        int count = 0;
        while(iterator.hasNext()) {
            Entry<Integer, String> next = iterator.next();
            System.out.println(next);
            assertThat(next.key, greaterThan((int)last));
            assertThat(next.key, greaterThanOrEqualTo(expectedFirstKey));
            assertThat(next.key, lessThanOrEqualTo(expectedLastKey));
            last = next.key;
            count++;
        }

        assertEquals(expectedSize, count);
        assertEquals(expectedLastKey, last);
    }
}