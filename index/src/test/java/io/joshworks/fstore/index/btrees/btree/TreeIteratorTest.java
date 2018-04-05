package io.joshworks.fstore.index.btrees.btree;

import io.joshworks.fstore.index.Entry;
import io.joshworks.fstore.index.btrees.storage.BlockStore;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TreeIteratorTest {

    private BlockStore<Node<Integer, String>> store;

    @Before
    public void setUp() {
        store = new BlockStore<>();
    }

    @Test
    public void empty_iterator() {
        BTree<Integer, String> btree = BTree.create(store, 2);
        assertFalse(btree.iterator().hasNext());
    }

    @Test
    public void empty_limitIterator() {
        BTree<Integer, String> btree = BTree.create(store, 2);
        assertFalse(btree.limitIterator(1000).hasNext());
    }

    @Test
    public void empty_rangeIterator() {
        BTree<Integer, String> btree = BTree.create(store, 2);
        assertFalse(btree.iterator(11111).hasNext());
    }

    @Test
    public void empty_rangeIterator_endExclusive() {
        BTree<Integer, String> btree = BTree.create(store, 2);
        assertFalse(btree.iterator(11111, 33333).hasNext());
    }

    @Test
    public void empty_limitIterator_endExclusive() {
        BTree<Integer, String> btree = BTree.create(store, 2);
        assertFalse(btree.limitIterator(11111, 33333).hasNext());
    }

    @Test(expected = IllegalArgumentException.class)
    public void empty_invalid_end_greaterThan_end() {
        BTree<Integer, String> btree = BTree.create(store, 2);
        btree.put(1, "");

        btree.iterator(55555, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void empty_null_startInclusive() {
        BTree<Integer, String> btree = BTree.create(store, 2);
        btree.put(1, "");

        btree.iterator(null);
    }

    @Test
    public void iterate() {
        BTree<Integer, String> btree = BTree.create(store, 2);
        for (int i = 1; i <= 500; i++) {
            btree.put(i, "");
        }
        Validator.validateNodes(store, btree.rootId, btree.order);

        Iterator<Entry<Integer, String>> iterator = btree.iterator();

        Integer last = null;
        while(iterator.hasNext()) {
            Integer value = iterator.next().key;
            System.out.println(value);
            if(last != null) {
                assertTrue(value > last);
            }
            last = value;
        }
    }

    @Test
    public void rangeIterator() {
        BTree<Integer, String> btree = BTree.create(store, 2);
        for (int i = 1; i <= 500; i++) {
            btree.put(i, "");
        }
        Validator.validateNodes(store, btree.rootId, btree.order);

        Iterator<Entry<Integer, String>> iterator = btree.iterator(250);


        Integer last = null;
        int count = 0;
        while(iterator.hasNext()) {
            count++;
            Integer value = iterator.next().key;
            System.out.println(value);
            if(last != null) {
                assertTrue(value > last);
            }
            last = value;
        }

        assertEquals(251, count); //start is inclusive

    }

    @Test
    public void limitIterator() {
        BTree<Integer, String> btree = BTree.create(store, 2);
        for (int i = 1; i <= 500; i++) {
            btree.put(i, "");
        }
        Validator.validateNodes(store, btree.rootId, btree.order);

        int limit = 10;
        Iterator<Entry<Integer, String>> iterator = btree.limitIterator(limit);

        Integer last = null;
        int count = 0;
        while(iterator.hasNext()) {
            count++;
            Integer value = iterator.next().key;
            System.out.println(value);
            if(last != null) {
                assertTrue(value > last);
            }
            last = value;
        }

        assertEquals(limit, count); //start is inclusive
    }

    @Test
    public void limitIterator_startingWith() {
        BTree<Integer, String> btree = BTree.create(store, 2);
        for (int i = 1; i <= 500; i++) {
            btree.put(i, "");
        }
        Validator.validateNodes(store, btree.rootId, btree.order);

        int limit = 10;
        int startingInclusive = 400;
        Iterator<Entry<Integer, String>> iterator = btree.limitIterator(startingInclusive, limit);

        Integer last = null;
        int count = 0;
        while(iterator.hasNext()) {
            count++;
            Integer value = iterator.next().key;
            assertTrue(value >= startingInclusive && value < startingInclusive + limit);

            System.out.println(value);
            if(last != null) {
                assertTrue(value > last);
            }
            last = value;
        }

        assertEquals(limit, count); //start is inclusive
    }

    @Test
    public void rangeIterator_startingWith_endsWith() {
        BTree<Integer, String> btree = BTree.create(store, 2);
        for (int i = 1; i <= 500; i++) {
            btree.put(i, "");
        }
        Validator.validateNodes(store, btree.rootId, btree.order);

        int startingInclusive = 400;
        int endExclusive = 450;
        Iterator<Entry<Integer, String>> iterator = btree.iterator(startingInclusive, endExclusive);

        Integer last = null;
        int count = 0;
        while(iterator.hasNext()) {
            count++;
            Integer value = iterator.next().key;
            assertTrue(value >= startingInclusive && value < endExclusive);

            System.out.println(value);
            if(last != null) {
                assertTrue(value > last);
            }
            last = value;
        }

        assertEquals(endExclusive - startingInclusive, count); //start is inclusive
    }

}