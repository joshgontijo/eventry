package io.joshworks.eventry.index;

import io.joshworks.eventry.index.midpoint.Midpoint;
import org.junit.Test;

import static org.junit.Assert.*;

public class MidpointTest {

    @Test
    public void when_comparing_with_same_indexEntry_then_return_zero() {
        Midpoint midpoint = new Midpoint(IndexEntry.of(1L, 1, 0), 0);
        assertEquals(0, midpoint.compareTo(IndexEntry.of(1L, 1, 0)));
    }

    @Test
    public void when_comparing_with_lesser_version_then_return_gt_zero() {
        Midpoint midpoint = new Midpoint(IndexEntry.of(1L, 2, 0), 0);
        assertTrue(midpoint.compareTo(IndexEntry.of(1L, 1, 0)) > 0);
    }

    @Test
    public void when_comparing_with_with_greater_version_then_return_lt_zero() {
        Midpoint midpoint = new Midpoint(IndexEntry.of(1L, 1, 0), 0);
        assertTrue(midpoint.compareTo(IndexEntry.of(1L, 2, 0)) < 0);
    }

    @Test
    public void when_comparing_with_with_lesser_stream_then_return_lt_zero() {
        Midpoint midpoint = new Midpoint(IndexEntry.of(2L, 1, 0), 0);
        assertTrue(midpoint.compareTo(IndexEntry.of(1L, 1, 0)) > 0);
    }

    @Test
    public void when_comparing_with_with_greater_stream_then_return_gt_zero() {
        Midpoint midpoint = new Midpoint(IndexEntry.of(1L, 1, 0), 0);
        assertTrue(midpoint.compareTo(IndexEntry.of(2L, 1, 0)) < 0);
    }

}