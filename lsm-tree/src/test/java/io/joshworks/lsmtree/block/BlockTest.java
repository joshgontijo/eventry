package io.joshworks.lsmtree.block;

import io.joshworks.fstore.serializer.StandardSerializer;
import org.junit.Test;

import static org.junit.Assert.*;

public class BlockTest {

    @Test
    public void add() {
        Block<String> block = new Block<>(StandardSerializer.of(String.class), 4);

        assertFalse(block.add("a"));
        assertFalse(block.add("a"));
        assertFalse(block.add("a"));

        assertTrue(block.add("a"));
        assertTrue(block.add("a"));
        assertTrue(block.add("a"));
    }

    @Test
    public void first() {
        Block<String> block = new Block<>(StandardSerializer.of(String.class), 4);
        block.add("a");
        block.add("b");

        assertEquals("a", block.first());
    }

    @Test
    public void size() {
        Block<String> block = new Block<>(StandardSerializer.of(String.class), 4);
        block.add("aa");
        block.add("bb");

        assertEquals(4, block.size());
    }

    @Test
    public void entries() {
        Block<String> block = new Block<>(StandardSerializer.of(String.class), 4);
        block.add("aa");
        block.add("bb");

        assertEquals(2, block.entries());
    }
}