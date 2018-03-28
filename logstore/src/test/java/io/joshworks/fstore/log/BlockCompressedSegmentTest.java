package io.joshworks.fstore.log;

import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.io.DiskStorage;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.serializer.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class BlockCompressedSegmentTest {

    private static final int BLOCK_SIZE = 16;
    private static final int BLOCK_BIT_SHIFT = 54;
    private static final int ENTRY_IDX_BIT_SHIFT = 10;

    private Log<String> log;
    private Path testFile;

    @Before
    public void setUp() {
        testFile = new File("test.db").toPath();
        Storage storage = new DiskStorage(testFile.toFile());
        log = BlockCompressedSegment.create(storage, new StringSerializer(), new SnappyCodec(), BLOCK_SIZE, BLOCK_BIT_SHIFT, ENTRY_IDX_BIT_SHIFT);
    }

    @After
    public void cleanup() {
        IOUtils.closeQuietly(log);
        Utils.tryDelete(testFile.toFile());
    }

    @Test
    public void get_not_flushed_should_be_null() {
        String value = "hello";
        long pos = log.append(value);
        String found = log.get(pos);
        assertNull(found);
    }

    @Test
    public void get_flushed() throws IOException {
        String value = "hello";
        long pos = log.append(value);
        log.flush();
        String found = log.get(pos);
        assertEquals(value, found);
    }

    @Test
    public void should_flush_after_maxSize() {
        String value = "1234567890123456";
        long pos = log.append(value);
        log.append("1");
        String found = log.get(pos);

        assertEquals(value, found);
    }

    @Test
    public void blockPosition() {
        long address = 100564646540L;
        int entryIdx = 5;

        BlockCompressedSegment cbls = (BlockCompressedSegment) log;
        long position = cbls.toBlockPosition(address, entryIdx);
        int positionOnBlock = cbls.getPositionOnBlock(position);
        long blockAddress = cbls.getBlockAddress(position);
        assertEquals(address, blockAddress);
        assertEquals(entryIdx, positionOnBlock);
    }

    @Test
    public void append() {
        String val = "a";
        long pos1 = log.append(val);
        assertEquals(0, pos1);

        long pos2 = log.append(val);
        assertEquals(1, pos2);

    }

    @Test
    public void get() throws IOException {
        List<Long> positions = new ArrayList<>();

        int items = 10;
        for (int i = 0; i < items; i++) {
            positions.add(log.append(String.valueOf(i)));
        }
        log.flush();

        for (int i = 0; i < items; i++) {
            String found = log.get(positions.get(i));
            assertEquals(String.valueOf(i), found);
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getWithLength() {
        log.get(1, 10);
    }


    @Test
    public void reader() throws IOException {
        List<String> addedItems = new ArrayList<>();
        int items = 1000;
        for (int i = 0; i < items; i++) {
            String item = String.valueOf(i);
            addedItems.add(item);
            log.append(item);
        }
        log.flush();

        int itemIdx = 0;
        for (String value : log.scanner()) {
            String expected = addedItems.get(itemIdx++);
            assertEquals(expected, value);
        }
        assertEquals(items, itemIdx);

    }

    @Test
    public void reader_position() throws IOException {
        List<String> addedItems = new ArrayList<>();
        List<Long> positions = new ArrayList<>();
        int items = 1000;
        for (int i = 0; i < items; i++) {
            String item = String.valueOf(i);
            addedItems.add(item);
            long position = log.append(item);
            positions.add(position);
        }
        log.flush();

        //creates a scanner starting at each position readFrom the log
        for (int i = 0; i < positions.size(); i++) {
            long pos = positions.get(i);
            int itemIdx = i;
            int foundItems = 0;
            for (String value : log.scanner(pos)) {
                String expected = addedItems.get(itemIdx++);
                assertEquals(expected, value);
                foundItems++;
            }
            assertEquals(addedItems.size() - i, foundItems);

        }




    }
}