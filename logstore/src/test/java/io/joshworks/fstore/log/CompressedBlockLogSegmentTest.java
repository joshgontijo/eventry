package io.joshworks.fstore.log;

import io.joshworks.fstore.serializer.StringSerializer;
import io.joshworks.fstore.utils.IOUtils;
import io.joshworks.fstore.utils.io.DiskStorage;
import io.joshworks.fstore.utils.io.Storage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class CompressedBlockLogSegmentTest {

    private static final int BLOCK_SIZE = 16;

    private Log<String> log;
    private Path testFile;
    private Storage storage;

    @Before
    public void setUp() {
        testFile = new File("test.db").toPath();
        storage = new DiskStorage(testFile.toFile());
        log = CompressedBlockLogSegment.create(storage, new StringSerializer(), new SnappyCodec(), BLOCK_SIZE);
    }

    @After
    public void cleanup() {
        IOUtils.closeQuietly(storage);
        IOUtils.closeQuietly(log);
        Utils.tryRemoveFile(testFile.toFile());
    }

    @Test
    public void append_position() {
        String value = "hello";
        long pos = log.append(value);
        assertEquals(0, pos);

        pos = log.append(value);
        assertEquals(5, pos);
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
        long position = CompressedBlockLogSegment.toAbsolutePosition(address, entryIdx);
        int positionOnBlock = CompressedBlockLogSegment.getPositionOnBlock(position);
        long blockAddress = CompressedBlockLogSegment.getBlockAddress(position);
        assertEquals(address, blockAddress);
        assertEquals(entryIdx, positionOnBlock);

    }

    @Test
    public void multiple_blocks() throws IOException {
        List<Long> positions = new LinkedList<>();

        long writeStart = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
            positions.add(log.append(String.valueOf(i)));
        }
        log.flush();
        System.out.println("WRITE: " + (System.currentTimeMillis() - writeStart));


        long readStart = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
            String found = log.get(positions.get(i));
            assertEquals(String.valueOf(i), found);
        }
        System.out.println("READ: " + (System.currentTimeMillis() - readStart));
    }

}