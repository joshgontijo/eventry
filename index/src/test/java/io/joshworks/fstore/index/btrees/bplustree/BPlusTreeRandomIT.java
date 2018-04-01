package io.joshworks.fstore.index.btrees.bplustree;

import io.joshworks.fstore.index.btrees.storage.BlockStore;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static io.joshworks.fstore.index.btrees.util.FileHelper.saveTestData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class BPlusTreeRandomIT {


    /**
     * Runs with different parameters until failure, when it fails, saves the values used for later replication
     */
    @Test
    @Ignore
    public void until_failure_uniqueInsert_randomDelete() throws IOException {

        for (long numKeys = 1; numKeys < Integer.MAX_VALUE; numKeys+= 100000) {
            for (long order = 3; order < 1000; order+= 10) {
//                System.out.println(MessageFormat.format("Testing... order: {0}, numKeys: {1}", order, numKeys));
                BPlusTree<Integer, String> btree = BPlusTree.of(new BlockStore<>(0), (int)order);
                LinkedList<Integer> data = generateUniqueUnorderedData(numKeys);
                insert(btree, new LinkedList<>(data), false);

                Collections.shuffle(data);
                remove(btree, data, false);
            }
        }
    }

    static void insert(BPlusTree<Integer, String> btree, LinkedList<Integer> data, boolean isReplay) throws IOException {
        for (Integer val : data) {
            try {
                String old = btree.put(val, String.valueOf(val));

                if (old != null) { //Unique values insertion only
                    if (!isReplay)
                        saveTestData(btree.order, data, val);
                    fail("Element " + val + " wasn't inserted");

                }

            } catch (Exception e) {
                if (!isReplay)
                    saveTestData(btree.order, data, val);
                throw e;
            }
        }

        if (!isReplay & data.size() != btree.size()) {
            saveTestData(btree.order, data, null);
        }
        assertEquals(data.size(), btree.size());
    }

    static void remove(BPlusTree<Integer, String> btree, LinkedList<Integer> data, boolean isReplay) throws IOException {
        for (Integer value : data) {
            try {
                String deleted = btree.remove(value);
                if (deleted == null) {
                    if (!isReplay)
                        saveTestData(btree.order, data, value);
                    fail("Element " + value + " wasn't deleted");
                }

                if (!String.valueOf(value).equals(deleted)) {
                    if (!isReplay)
                        saveTestData(btree.order, data, value);
                    fail("Expected removed value is '" + value + "', got '" + deleted + "'");
                }

            } catch (Exception e) {
                if (!isReplay)
                    saveTestData(btree.order, data, value);
                throw e;
            }
        }

        if (!isReplay && btree.size() != 0) {
            saveTestData(btree.order, data, null);
        }
        assertEquals(0, btree.size());
    }


    private LinkedList<Integer> generateUniqueUnorderedData(long size) {
        Set<Integer> data = new HashSet<>();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        while (data.size() < size) {
            data.add(random.nextInt());
        }
        return new LinkedList<>(data);
    }


}