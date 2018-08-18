package io.joshworks.fstore.es.utils;

import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.LogIterator;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IteratorsTest {

    @Test
    public void reversed() {
    }

    @Test
    public void concat() {
        LogIterator<Integer> first = Iterators.of(List.of(1));
        LogIterator<Integer> second = Iterators.of(List.of(2));

        LogIterator<Integer> concat = Iterators.concat(List.of(first, second));

        assertTrue(concat.hasNext());
        assertEquals(Integer.valueOf(1), concat.next());

        assertTrue(concat.hasNext());
        assertEquals(Integer.valueOf(2), concat.next());

        assertFalse(concat.hasNext());

    }

    @Test
    public void concat_empty() {
        LogIterator<Integer> first = Iterators.empty();
        LogIterator<Integer> second = Iterators.empty();

        Iterator<Integer> concat = Iterators.concat(List.of(first, second));

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
        LogIterator<Integer> first = Iterators.of(List.of(1));
        LogIterator<Integer> second = Iterators.empty();

        Iterator<Integer> concat = Iterators.concat(List.of(first, second));

        assertTrue(concat.hasNext());
        assertEquals(Integer.valueOf(1), concat.next());

        assertFalse(concat.hasNext());
        assertFalse(concat.hasNext());
        assertFalse(concat.hasNext());
        assertFalse(concat.hasNext());
    }

    @Test
    public void concat_one_of_second_item_empty() {
        LogIterator<Integer> first = Iterators.of(List.of(1));
        LogIterator<Integer> second = Iterators.empty();
        LogIterator<Integer> third = Iterators.of(List.of(2));

        Iterator<Integer> concat = Iterators.concat(List.of(first, second, third));

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