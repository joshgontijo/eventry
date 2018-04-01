package io.joshworks.fstore.index.btrees.btree;

import io.joshworks.fstore.index.btrees.storage.BlockStore;
import io.joshworks.fstore.index.btrees.util.TestData;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static io.joshworks.fstore.index.btrees.btree.BTreeRandomIT.insert;
import static io.joshworks.fstore.index.btrees.btree.BTreeRandomIT.remove;
import static io.joshworks.fstore.index.btrees.util.FileHelper.all;
import static io.joshworks.fstore.index.btrees.util.FileHelper.latest;
import static io.joshworks.fstore.index.btrees.util.FileHelper.loadTestData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class BTreeTest {

    private BlockStore<Node<Integer, String>> store;

    @Before
    public void setUp() {
        store = new BlockStore<>(0);
    }

    @Test
    public void insert_nonFull() {
        BTree<Integer, String> btree = BTree.create(store, 2);
        btree.put(1, "a");
        btree.put(2, "b");
        btree.put(3, "c");

        Validator.validateNodes(store, btree.rootId, btree.order);

        assertEquals("a", btree.get(1));
        assertEquals("b", btree.get(2));
        assertEquals("c", btree.get(3));
    }

    @Test
    public void insert_full() {
        BTree<Integer, String> btree = BTree.create(store, 2);
        for (int i = 1; i <= 10; i++) {
            btree.put(i, String.valueOf(i));
        }
        for (int i = 1; i <= 10; i++) {
            assertEquals(String.valueOf(i), btree.get(i));
        }
        Validator.validateNodes(store, btree.rootId, btree.order);
    }

    @Test
    public void insert_non_sequential() {
        BTree<Integer, String> btree = BTree.create(store, 2);
        int[] values = new int[]{56, -234, 1, 888, 13455, 31, 99999, -987654, 32, 111};
        for (int value : values) {
            btree.put(value, String.valueOf(value));
        }

        for (int value : values) {
            assertEquals(String.valueOf(value), btree.get(value));
        }
        Validator.validateNodes(store, btree.rootId, btree.order);
    }

    @Test
    public void delete() {
        BTree<Integer, String> btree = BTree.create(store, 2);
        btree.put(1, "1");
        btree.put(2, "2");

        removeItem(btree, 1);

        assertNull(btree.get(1));
        assertEquals("2", btree.get(2));
    }


    @Test
    public void delete_1_leaf() {
        BTree<Integer, String> btree = BTree.create(store, 2);
        for (int i = 1; i <= 10; i++) {
            btree.put(i, String.valueOf(i));
        }

        removeItem(btree, 1);
        assertRemovedItem(btree, 1);
    }

    private void removeItem(BTree<Integer, String> btree, int key) {
        Validator.validateNodes(store, btree.rootId, btree.order);
        String removed = btree.remove(key);
        assertEquals(String.valueOf(key), removed);
        Validator.validateNodes(store, btree.rootId, btree.order);
    }

    @Test
    public void delete_2_internal() {
        BTree<Integer, String> btree = BTree.create(store, 2);
        for (int i = 1; i <= 10; i++) {
            btree.put(i, String.valueOf(i));
        }

        removeItem(btree, 2);
        assertRemovedItem(btree, 2);
    }

    @Test
    public void delete_3_leaf() {
        BTree<Integer, String> btree = BTree.create(store, 2);
        for (int i = 1; i <= 10; i++) {
            btree.put(i, String.valueOf(i));
        }

        removeItem(btree, 3);

        assertRemovedItem(btree, 3);
    }


    @Test
    public void delete_4_root() {
        BTree<Integer, String> btree = BTree.create(store, 2);
        for (int i = 1; i <= 10; i++) {
            btree.put(i, String.valueOf(i));
        }

        removeItem(btree, 4);
        assertRemovedItem(btree, 4);
    }

    @Test
    public void delete_5_leaf() {
        BTree<Integer, String> btree = BTree.create(store, 2);
        for (int i = 1; i <= 10; i++) {
            btree.put(i, String.valueOf(i));
        }

        removeItem(btree, 5);
        assertRemovedItem(btree, 5);
    }

    @Test
    public void delete_6_internal() {
        BTree<Integer, String> btree = BTree.create(store, 2);
        for (int i = 1; i <= 10; i++) {
            btree.put(i, String.valueOf(i));
        }

        removeItem(btree, 6);
        assertRemovedItem(btree, 6);
    }

    @Test
    public void delete_7_leaf() {
        BTree<Integer, String> btree = BTree.create(store, 2);
        for (int i = 1; i <= 10; i++) {
            btree.put(i, String.valueOf(i));
        }

        removeItem(btree, 7);
        assertRemovedItem(btree, 7);
    }

    @Test
    public void delete_8_internal() {
        BTree<Integer, String> btree = BTree.create(store, 2);
        for (int i = 1; i <= 10; i++) {
            btree.put(i, String.valueOf(i));
        }

        removeItem(btree, 8);
        assertRemovedItem(btree, 8);
    }

    @Test
    public void delete_9_leaf() {
        BTree<Integer, String> btree = BTree.create(store, 2);
        for (int i = 1; i <= 10; i++) {
            btree.put(i, String.valueOf(i));
        }

        removeItem(btree, 9);
        assertRemovedItem(btree, 9);
    }

    @Test
    public void delete_10_leaf() {
        BTree<Integer, String> btree = BTree.create(store, 2);
        for (int i = 1; i <= 10; i++) {
            btree.put(i, String.valueOf(i));
        }

        removeItem(btree, 10);
        assertRemovedItem(btree, 10);
    }


    @Test
    public void delete_all_10() {
        BTree<Integer, String> btree = BTree.create(store, 2);
        for (int i = 1; i <= 10; i++) {
            btree.put(i, String.valueOf(i));
        }
        for (int i = 1; i <= 10; i++) {
            String removed = btree.remove(i);
            assertEquals(String.valueOf(i), removed);
            Validator.validateNodes(store, btree.rootId, btree.order);
        }
        for (int i = 1; i <= 10; i++) {
            assertNull(btree.get(i));
        }
    }

    @Test
    public void delete_all_500() {
        BTree<Integer, String> btree = BTree.create(store, 2);
        for (int i = 1; i <= 500; i++) {
            btree.put(i, String.valueOf(i));
        }
        Validator.validateNodes(store, btree.rootId, btree.order);
        for (int i = 1; i <= 500; i++) {
            assertNotNull(btree.remove(i));
            Validator.validateNodes(store, btree.rootId, btree.order);
        }
        for (int i = 1; i <= 500; i++) {
            assertNull(btree.get(i));
        }
    }

    @Test
    public void delete_reverse() {
        BTree<Integer, String> btree = BTree.create(store, 2);
        for (int i = 1; i <= 10; i++) {
            btree.put(i, String.valueOf(i));
        }
        try {
            for (int i = 10; i >= 1; i--) {
                assertNotNull(btree.remove(i));
                Validator.validateNodes(store, btree.rootId, btree.order);
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
        BTree<Integer, String> btree = BTree.create(store, 2);
        for (int i = 1; i <= 10; i++) {
            btree.put(i, String.valueOf(i));
            assertEquals(String.valueOf(i), btree.get(i));
        }

        try {
            for (int i = 1; i <= 10; i++) {
                assertNotNull(btree.remove(i));
                assertNull(btree.get(i));
                Validator.validateNodes(store, btree.rootId, btree.order);
            }

        } catch (Exception e) {
            System.err.println(btree.toString());
            throw e;
        }
    }

    @Test
    public void size() {
        BTree<Integer, String> btree = BTree.create(store, 3);
        int entries = 500000;
        for (int i = 0; i < entries; i++) {
            btree.put(i, String.valueOf(i));
        }
        assertEquals(entries, btree.size());
        Validator.validateNodes(store, btree.rootId, btree.order);
    }

    @Test
    public void insert_many() {
        BTree<Integer, String> btree = BTree.create(store, 3);
        int entries = 500000;
        for (int i = 1; i < entries; i++) {
            btree.put(i, String.valueOf(i));
        }

        Validator.validateNodes(store, btree.rootId, btree.order);

        for (int i = 1; i < entries; i++) {
            String found = btree.get(i);
            assertEquals(String.valueOf(i), found);
        }
    }

    @Test
    public void validateEmpty() {
        BTree<Integer, String> btree = BTree.create(store, 3);
        Validator.validateNodes(store, btree.rootId, btree.order);
    }

    @Test
    public void insert_duplicate() {
        BTree<Integer, String> btree = BTree.create(store, 3);
        assertNull(btree.put(1, "a"));
        assertEquals("a", btree.put(1, "a"));
    }

    /**
     * Rerun test using the file
     */
    @Test
    public void replay_last() throws IOException {
        testFromFile(latest());
    }

    /**
     * Rerun all previous failed tests files
     */
    @Test
    public void regression() throws IOException {
        for (String file : all()) {
            testFromFile(file);
        }
    }

    private void testFromFile(String fileName) throws IOException {
        if(fileName == null) {
            System.out.println("No previous regression failed test file found...");
            return;
        }
        try {
            System.out.println(":: Running test on " + fileName + " ::");
            TestData testData = loadTestData(fileName);
            BTree<Integer, String> btree = BTree.create(new BlockStore<>(0), testData.order);

            insert(btree, testData.data, true);
            remove(btree, testData.data, true);

        } catch (Exception e) {
            System.err.println("Failed on file " + fileName);
            throw e;
        }
    }

    //Considering a tree with range values from 1 to 10
    private void assertRemovedItem(BTree<Integer, String> btree, int removed) {
        for (int i = 1; i <= 10; i++) {
            if (i == removed) {
                assertNull(btree.get(i));
            } else {
                assertEquals(String.valueOf(i), btree.get(i));

            }

        }
    }

}