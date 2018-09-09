package io.joshworks.eventry.index;

import org.junit.Test;

import static org.junit.Assert.*;

public class IndexEntryTest {

    @Test(expected = IllegalArgumentException.class)
    public void version_must_start_from_zero() {
        IndexEntry.of(0, -1, 0);
    }

    @Test
    public void higher_stream_version_is_greater() {

        IndexEntry first = IndexEntry.of(1, 1, 0);
        IndexEntry second = IndexEntry.of(1, 2, 0);

        assertTrue(first.compareTo(second) < 0);
    }

    @Test
    public void higher_stream_hash_is_greater() {

        IndexEntry first = IndexEntry.of(1, 1, 0);
        IndexEntry second = IndexEntry.of(2, 1, 0);

        assertTrue(first.compareTo(second) < 0);
    }

    @Test
    public void position_is_not_considered_in_compareTo() {

        IndexEntry first = IndexEntry.of(1, 1, 0);
        IndexEntry second = IndexEntry.of(1, 1, 1);

        assertEquals(0, first.compareTo(second));
    }
}