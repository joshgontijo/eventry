package io.joshworks.fstore.log.block;

import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.log.Utils;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.serializer.Serializers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BlockTest {

    private Path tempFile;
    private LogAppender<Block<String>> appender;


    @Before
    public void setUp() throws Exception {
        tempFile = Files.createTempDirectory(null);

        Serializer<Block<String>> blockSerializer = new CompressedBlockSerializer<>(new SnappyCodec(), Serializers.STRING);
        appender = LogAppender.builder(tempFile.toFile(), blockSerializer).segmentSize(5242880).open();
    }

    @After
    public void tearDown() {
        IOUtils.closeQuietly(appender);
        Utils.tryDelete(tempFile.toFile());
    }

    @Test
    public void when_the_limit_is_exceeded_return_true() {

        Block<String> block = Block.newBlock(Serializers.STRING, 4);
        assertFalse(block.add("a"));
        assertFalse(block.add("a"));
        assertFalse(block.add("a"));
        assertTrue(block.add("a"));
    }

    @Test
    public void entries_return_all_stored_items() {

        Block<String> block = Block.newBlock(Serializers.STRING, 4);
        assertFalse(block.add("a"));
        assertFalse(block.add("b"));
        assertFalse(block.add("c"));
        assertTrue(block.add("d"));

        List<String> entries = block.entries();
        assertEquals(4, entries.size());
    }

    @Test
    public void entryCount() {

        Block<String> block = Block.newBlock(Serializers.STRING, 4);
        assertFalse(block.add("a"));
        assertFalse(block.add("b"));

        assertEquals(2, block.entryCount());
    }

    @Test
    public void first() {
        Block<String> block = Block.newBlock(Serializers.STRING, 4);
        assertFalse(block.add("a"));
        assertFalse(block.add("b"));

        assertEquals("a", block.first());
    }

    @Test
    public void last() {
        Block<String> block = Block.newBlock(Serializers.STRING, 4);
        assertFalse(block.add("a"));
        assertFalse(block.add("b"));

        assertEquals("b", block.last());
    }

    @Test
    public void uncompressedSize() {
        Block<String> block = Block.newBlock(Serializers.STRING, 4);
        assertFalse(block.add("a"));

        assertEquals(12, block.uncompressedSize());
    }

    @Test
    public void iterator() {
        Block<String> block = Block.newBlock(Serializers.STRING, 4);
        assertFalse(block.add("a"));
        assertFalse(block.add("b"));
        assertFalse(block.add("c"));

        Iterator<String> it = block.iterator();

        assertTrue(it.hasNext());
        assertEquals("a", it.next());

        assertTrue(it.hasNext());
        assertEquals("b", it.next());

        assertTrue(it.hasNext());
        assertEquals("c", it.next());

        assertFalse(it.hasNext());
    }

    @Test
    public void block_returns_the_same_sata_after_serialization() {
        Block<String> block = Block.newBlock(Serializers.STRING, 4);
        block.add("a");
        block.add("b");
        block.add("c");

        long blockPos = appender.append(block);

        Block<String> found = appender.get(blockPos);

        assertEquals(3, found.entryCount());
        List<String> entries = found.entries();

        assertEquals(3, entries.size());
        assertEquals("a", entries.get(0));
        assertEquals("b", entries.get(1));
        assertEquals("c", entries.get(2));
    }

    @Test
    public void insert_multiple_blocks() {

        List<Long> blockAddresses = new ArrayList<>();

        //given
        int numEntries = 100000;
        int blockSize = 4096;

        //when
        Block<String> block = Block.newBlock(Serializers.STRING, blockSize);
        for (int i = 0; i < numEntries; i++) {
            if (block.add(String.valueOf(i))) {
                long blockAddress = appender.append(block);
                blockAddresses.add(blockAddress);
                block = Block.newBlock(Serializers.STRING, 4096);
            }
        }
        long address = appender.append(block);
        blockAddresses.add(address);


        //then
        int foundEntries = 0;

        for (Long blockAddress : blockAddresses) {
            Block<String> foundBlock = appender.get(blockAddress);
            for (String value : foundBlock.entries()) {
                assertEquals(String.valueOf(foundEntries), value);
                foundEntries++;
            }
        }

        assertEquals(numEntries, foundEntries);
    }

}