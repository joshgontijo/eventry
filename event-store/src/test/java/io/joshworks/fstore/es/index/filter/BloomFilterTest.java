package io.joshworks.fstore.es.index.filter;

import io.joshworks.fstore.es.Utils;
import io.joshworks.fstore.index.filter.Hash;
import io.joshworks.fstore.serializer.Serializers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BloomFilterTest {

    private File testFolder;
    private BloomFilter<Long> filter;


    @Before
    public void setUp() {
        testFolder = Utils.testFolder();
        filter = openFilter();

    }

    @After
    public void tearDown() {
        Utils.tryDelete(testFolder);
    }

    private BloomFilter<Long> openFilter() {
        return BloomFilter.openOrCreate(testFolder, "segmentA", 100, 0.01, new Hash.Murmur64<>(Serializers.LONG));
    }

    @Test
    public void contains() {
        filter.add(1L);
        assertTrue(filter.contains(1L));
    }

    @Test
    public void loaded_filter_is_identical_in_size_and_set_bits() {
        filter.add(1L);
        assertTrue(filter.contains(1L));
        assertFalse(filter.contains(2L));

        filter.write();

        BloomFilter<Long> loaded = openFilter();

        assertEquals(filter.hashes, loaded.hashes);
        assertEquals(filter.hashes.size(), loaded.hashes.size());
        assertEquals(filter.hashes.length(), loaded.hashes.length());
        assertEquals(filter.hashes.hashCode(), loaded.hashes.hashCode());
        assertTrue(Arrays.equals(filter.hashes.toByteArray(), loaded.hashes.toByteArray()));

        assertTrue(loaded.contains(1L));
        assertFalse(loaded.contains(2L));
    }


}