package io.joshworks.fstore.index.btrees.bplustree;

import io.joshworks.fstore.index.btrees.storage.BlockStore;
import io.joshworks.fstore.index.btrees.util.TestData;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static io.joshworks.fstore.index.btrees.bplustree.BPlusTreeRandomIT.insert;
import static io.joshworks.fstore.index.btrees.bplustree.BPlusTreeRandomIT.remove;
import static io.joshworks.fstore.index.btrees.util.FileHelper.all;
import static io.joshworks.fstore.index.btrees.util.FileHelper.loadTestData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class BPlusTreeTest {

    private BlockStore<Node<Integer, String>> store;

    @Before
    public void setUp() {
        store = new BlockStore<>(0);
    }

    @Test
    public void insert_nonFull() {
        BPlusTree<Integer, String> btree = BPlusTree.of(store, 3);
        btree.put(1, "a");
        btree.put(2, "b");
        btree.put(3, "c");


        assertEquals("a", btree.get(1));
        assertEquals("b", btree.get(2));
        assertEquals("c", btree.get(3));
    }

    @Test
    public void insert_full() {
        BPlusTree<Integer, String> btree = BPlusTree.of(store, 3);
        for (int i = 1; i <= 10; i++) {
            btree.put(i, String.valueOf(i));
        }
        for (int i = 1; i <= 10; i++) {
            assertEquals(String.valueOf(i), btree.get(i));
        }
    }

    @Test
    public void insert_non_sequential() {
        BPlusTree<Integer, String> btree = BPlusTree.of(store, 3);
        int[] values = new int[]{56, -234, 1, 888, 13455, 31, 99999, -987654, 32, 111};
        for (int value : values) {
            btree.put(value, String.valueOf(value));
        }

        for (int value : values) {
            assertEquals(String.valueOf(value), btree.get(value));
        }
    }

    @Test
    public void delete() {
        BPlusTree<Integer, String> btree = BPlusTree.of(store, 3);
        btree.put(1, "1");
        btree.put(2, "2");

        removeItem(btree, 1);

        assertNull(btree.get(1));
        assertEquals("2", btree.get(2));
    }


    @Test
    public void delete_1_leaf() {
        BPlusTree<Integer, String> btree = BPlusTree.of(store, 3);
        for (int i = 1; i <= 10; i++) {
            btree.put(i, String.valueOf(i));
        }

        removeItem(btree, 1);
        assertRemovedItem(btree, 1);
    }

    private void removeItem(BPlusTree<Integer, String> btree, int key) {
        String removed = btree.remove(key);
        assertEquals(String.valueOf(key), removed);
    }

    @Test
    public void delete_2_internal() {
        BPlusTree<Integer, String> btree = BPlusTree.of(store, 3);
        for (int i = 1; i <= 10; i++) {
            btree.put(i, String.valueOf(i));
        }

        removeItem(btree, 2);
        assertRemovedItem(btree, 2);
    }

    @Test
    public void delete_3_leaf() {
        BPlusTree<Integer, String> btree = BPlusTree.of(store, 3);
        for (int i = 1; i <= 10; i++) {
            btree.put(i, String.valueOf(i));
        }

        removeItem(btree, 3);

        assertRemovedItem(btree, 3);
    }


    @Test
    public void delete_4_root() {
        BPlusTree<Integer, String> btree = BPlusTree.of(store, 3);
        for (int i = 1; i <= 10; i++) {
            btree.put(i, String.valueOf(i));
        }

        removeItem(btree, 4);
        assertRemovedItem(btree, 4);
    }

    @Test
    public void delete_5_leaf() {
        BPlusTree<Integer, String> btree = BPlusTree.of(store, 3);
        for (int i = 1; i <= 10; i++) {
            btree.put(i, String.valueOf(i));
        }

        removeItem(btree, 5);
        assertRemovedItem(btree, 5);
    }

    @Test
    public void delete_6_internal() {
        BPlusTree<Integer, String> btree = BPlusTree.of(store, 3);
        for (int i = 1; i <= 10; i++) {
            btree.put(i, String.valueOf(i));
        }

        removeItem(btree, 6);
        assertRemovedItem(btree, 6);
    }

    @Test
    public void delete_7_leaf() {
        BPlusTree<Integer, String> btree = BPlusTree.of(store, 3);
        for (int i = 1; i <= 10; i++) {
            btree.put(i, String.valueOf(i));
        }

        removeItem(btree, 7);
        assertRemovedItem(btree, 7);
    }

    @Test
    public void delete_8_internal() {
        BPlusTree<Integer, String> btree = BPlusTree.of(store, 3);
        for (int i = 1; i <= 10; i++) {
            btree.put(i, String.valueOf(i));
        }

        removeItem(btree, 8);
        assertRemovedItem(btree, 8);
    }

    @Test
    public void delete_9_leaf() {
        BPlusTree<Integer, String> btree = BPlusTree.of(store, 3);
        for (int i = 1; i <= 10; i++) {
            btree.put(i, String.valueOf(i));
        }

        removeItem(btree, 9);
        assertRemovedItem(btree, 9);
    }

    @Test
    public void delete_10_leaf() {
        BPlusTree<Integer, String> btree = BPlusTree.of(store, 3);
        for (int i = 1; i <= 10; i++) {
            btree.put(i, String.valueOf(i));
        }

        removeItem(btree, 10);
        assertRemovedItem(btree, 10);
    }


    @Test
    public void delete_all_10() {
        BPlusTree<Integer, String> btree = BPlusTree.of(store, 3);
        for (int i = 1; i <= 10; i++) {
            btree.put(i, String.valueOf(i));
        }
        for (int i = 1; i <= 10; i++) {
            String removed = btree.remove(i);
            assertEquals(String.valueOf(i), removed);
        }
        for (int i = 1; i <= 10; i++) {
            assertNull(btree.get(i));
        }
    }

    @Test
    public void delete_all_500() {
        BPlusTree<Integer, String> btree = BPlusTree.of(store, 3);
        for (int i = 1; i <= 500; i++) {
            btree.put(i, String.valueOf(i));
        }
        for (int i = 1; i <= 500; i++) {
            assertNotNull(btree.remove(i));
        }
        for (int i = 1; i <= 500; i++) {
            assertNull(btree.get(i));
        }
    }

    @Test
    public void delete_reverse() {
        BPlusTree<Integer, String> btree = BPlusTree.of(store, 3);
        for (int i = 1; i <= 10; i++) {
            btree.put(i, String.valueOf(i));
        }
        try {
            for (int i = 10; i >= 1; i--) {
                assertNotNull(btree.remove(i));
            }

        } catch (Exception e) {
            System.err.println(btree.toString());
            throw e;
        }
        for (int i = 1; i <= 10; i++) {
            assertNull(btree.get(i));
        }
    }

    @Test
    public void delete_internal1() {
        BPlusTree<Integer, String> btree = BPlusTree.of(store, 3);
        for (int i = 1; i <= 10; i++) {
            btree.put(i, String.valueOf(i));
            assertEquals(String.valueOf(i), btree.get(i));
        }

        try {
            for (int i = 1; i <= 10; i++) {
                assertNotNull(btree.remove(i));
                assertNull(btree.get(i));
            }

        } catch (Exception e) {
            System.err.println(btree.toString());
            throw e;
        }
    }

    @Test
    public void size() {
        BPlusTree<Integer, String> btree = BPlusTree.of(store, 3);
        int entries = 500000;
        for (int i = 0; i < entries; i++) {
            btree.put(i, String.valueOf(i));
        }
        assertEquals(entries, btree.size());
    }

    @Test
    public void insert_many() {
        BPlusTree<Integer, String> btree = BPlusTree.of(store, 3);
        int entries = 500000;
        for (int i = 1; i < entries; i++) {
            btree.put(i, String.valueOf(i));
        }


        for (int i = 1; i < entries; i++) {
            String found = btree.get(i);
            assertEquals(String.valueOf(i), found);
        }
    }

    @Test
    public void validateEmpty() {
        BPlusTree<Integer, String> btree = BPlusTree.of(store, 3);
        assertTrue(btree.isEmpty());
        btree.put(1, "a");
        assertFalse(btree.isEmpty());
    }

    @Test
    public void insert_duplicate() {
        BPlusTree<Integer, String> btree = BPlusTree.of(store, 3);
        assertNull(btree.put(1, "a"));
        assertEquals("a", btree.put(1, "b"));

        assertEquals("b", btree.get(1));
    }

//    /**
//     * Rerun test using the file
//     */
//    @Test
//    public void replay_last() throws IOException {
//        testFromFile(latest());
//    }

    /**
//     * Rerun all previous failed tests files
//     */
    @Test
    public void regression() throws IOException {
        for (String file : all()) {
            testFromFile(file);
        }
    }

    private void testFromFile(String fileName) throws IOException {
        try {
            System.out.println(":: Running test on " + fileName + " ::");
            TestData testData = loadTestData(fileName);
            BPlusTree<Integer, String> btree = BPlusTree.of(new BlockStore<>(0), testData.order);

            insert(btree, testData.data, true);
            remove(btree, testData.data, true);

        } catch (Exception e) {
            System.err.println("Failed on file " + fileName);
            throw e;
        }
    }

    //Considering a tree with range values from 1 to 10
    private void assertRemovedItem(BPlusTree<Integer, String> btree, int removed) {
        for (int i = 1; i <= 10; i++) {
            if (i == removed) {
                assertNull(btree.get(i));
            } else {
                assertEquals(String.valueOf(i), btree.get(i));

            }

        }
    }

}