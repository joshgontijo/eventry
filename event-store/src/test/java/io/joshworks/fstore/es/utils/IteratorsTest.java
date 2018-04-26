package io.joshworks.fstore.es.utils;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import static org.junit.Assert.*;

public class IteratorsTest {

    @Test
    public void reversed() {
    }

    @Test
    public void concat() {
        Iterator<Integer> first = Arrays.asList(1).iterator();
        Iterator<Integer> second = Arrays.asList(2).iterator();

        Iterator<Integer> concat = Iterators.concat(Arrays.asList(first, second));

        assertTrue(concat.hasNext());
        assertEquals(Integer.valueOf(1), concat.next());

        assertTrue(concat.hasNext());
        assertEquals(Integer.valueOf(2), concat.next());

        assertFalse(concat.hasNext());

    }

    @Test
    public void concat_empty() {
        Iterator<Integer> first = Collections.<Integer>emptyList().iterator();
        Iterator<Integer> second = Collections.<Integer>emptyList().iterator();

        Iterator<Integer> concat = Iterators.concat(Arrays.asList(first, second));

        assertFalse(concat.hasNext());
        assertFalse(concat.hasNext());
        assertFalse(concat.hasNext());
        assertFalse(concat.hasNext());
        assertFalse(concat.hasNext());
        assertFalse(concat.hasNext());
        assertFalse(concat.hasNext());
        assertFalse(concat.hasNext());
        assertFalse(concat.hasNext());
        assertFalse(concat.hasNext());

    }

    @Test
    public void concat_one_of_lists_empty() {
        Iterator<Integer> first = Arrays.asList(1).iterator();
        Iterator<Integer> second = Collections.<Integer>emptyList().iterator();

        Iterator<Integer> concat = Iterators.concat(Arrays.asList(first, second));

        assertTrue(concat.hasNext());
        assertEquals(Integer.valueOf(1), concat.next());

        assertFalse(concat.hasNext());
        assertFalse(concat.hasNext());
        assertFalse(concat.hasNext());
        assertFalse(concat.hasNext());
    }

    @Test
    public void concat_one_of_second_item_empty() {
        Iterator<Integer> first = Arrays.asList(1).iterator();
        Iterator<Integer> second = Collections.<Integer>emptyList().iterator();
        Iterator<Integer> third = Arrays.asList(2).iterator();

        Iterator<Integer> concat = Iterators.concat(Arrays.asList(first, second, third));

        assertTrue(concat.hasNext());
        assertEquals(Integer.valueOf(1), concat.next());

        assertTrue(concat.hasNext());
        assertEquals(Integer.valueOf(2), concat.next());

        assertFalse(concat.hasNext());
        assertFalse(concat.hasNext());
        assertFalse(concat.hasNext());
        assertFalse(concat.hasNext());

    }

}