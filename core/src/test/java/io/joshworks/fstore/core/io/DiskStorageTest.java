package io.joshworks.fstore.core.io;

import io.joshworks.fstore.core.utils.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class DiskStorageTest {

    protected static final int DEFAULT_LENGTH = 5242880;
    protected static final String TEST_DATA = "TEST-DATA";
    protected Storage storage;
    protected File testFile;

    protected abstract Storage store(File file, long size);

    @Before
    public void setUp() {
        testFile = Utils.testFile();
        storage = store(testFile, DEFAULT_LENGTH);
    }

    @After
    public void cleanup() {
        IOUtils.closeQuietly(storage);
        Utils.tryDelete(testFile);
    }

    @Test(expected = IllegalArgumentException.class)
    public void when_witting_empty_data_an_exception_is_thrown() {
        storage.write(ByteBuffer.allocate(0));
    }

    @Test
    public void when_data_is_written_return_the_written_length() {
        ByteBuffer bb = ByteBuffer.wrap(TEST_DATA.getBytes(StandardCharsets.UTF_8));
        int written = storage.write(bb);
        assertEquals(TEST_DATA.length(), written);
    }

    @Test
    public void when_data_is_read_it_must_be_the_same_that_was_written() {
        ByteBuffer bb = ByteBuffer.wrap(TEST_DATA.getBytes(StandardCharsets.UTF_8));
        int write = storage.write(bb);

        ByteBuffer result = ByteBuffer.allocate(bb.capacity());
        int read = storage.read(0, result);

        assertEquals(write, read);
        assertTrue(Arrays.equals(bb.array(), result.array()));
    }

    @Test
    public void when_10000_entries_are_written_it_should_read_all_of_them() {
        int entrySize = 36; //uuid byte position

        int items = 10000;
        Set<String> inserted = new HashSet<>();
        for (int i = 0; i < items; i++) {
            String val = UUID.randomUUID().toString();
            inserted.add(val);
            storage.write(ByteBuffer.wrap(val.getBytes(StandardCharsets.UTF_8)));
        }

        long offset = 0;
        int itemsRead = 0;
        for (int i = 0; i < items; i++) {
            ByteBuffer bb = ByteBuffer.allocate(entrySize);
            int read = storage.read(offset, bb);
            assertEquals("Failed on iteration " + i, entrySize, read);

            bb.flip();
            String found = new String(bb.array(), StandardCharsets.UTF_8);
            assertTrue("Not found: [" + found + "] at offset " + offset + ", iteration: " + i, inserted.contains(found));
            itemsRead++;
            offset += entrySize;
        }

        assertEquals(items, itemsRead);
    }

    @Test
    public void when_writing_large_entry_it_should_read_the_same_value() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            sb.append(UUID.randomUUID().toString());
        }

        String longString = sb.toString();

        ByteBuffer bb = ByteBuffer.wrap(longString.getBytes(StandardCharsets.UTF_8));
        int write = storage.write(bb);

        ByteBuffer result = ByteBuffer.allocate(bb.capacity());
        int read = storage.read(0, result);

        assertEquals(write, read);
        assertTrue(Arrays.equals(bb.array(), result.array()));
    }

    @Test
    public void delete() throws Exception {

        File temp = Utils.testFile();
        try (Storage store = store(temp, DEFAULT_LENGTH)) {
            store.delete();
            assertFalse(Files.exists(temp.toPath()));
        } finally {
            Utils.tryDelete(temp);
        }
    }

    @Test
    public void when_data_is_written_the_size_must_increase() {
        int dataLength = (int) storage.length();

        byte[] data = new byte[dataLength];
        Arrays.fill(data, (byte) 1);
        ByteBuffer bb = ByteBuffer.wrap(data);

        storage.write(bb);
        bb.clear();
        storage.write(bb);

        assertTrue(storage.length() >= dataLength);

        ByteBuffer found = ByteBuffer.allocate(dataLength);
        int read = storage.read(0, found);

        assertEquals(dataLength, read);
        assertTrue(Arrays.equals(data, found.array()));
    }

    @Test
    public void providing_buffer_bigger_than_available_data_read_only_available() throws IOException {

        //given
        int size = 8;
        File temp = Utils.testFile();
        try (Storage store = store(temp, size)) {
            ByteBuffer data = ByteBuffer.allocate(size);
            data.putInt(1);
            data.putInt(2);
            data.flip();

            store.write(data);

            ByteBuffer result = ByteBuffer.allocate(500);

            //when
            int read = store.read(0, result);
            result.flip();

            //then
            assertEquals(size, read);
            assertEquals(size, result.remaining());
        } finally {
            Utils.tryDelete(temp);
        }
    }

    @Test
    public void position_is_updated_after_insert() {
        ByteBuffer bb = ByteBuffer.wrap("a".getBytes(StandardCharsets.UTF_8));

        assertEquals(0, storage.position());

        storage.write(bb);
        long pos = storage.position();

        assertEquals(bb.capacity(), pos);
    }


    @Test(expected = StorageException.class)
    public void truncate_readonly_throws_exception() {
        storage.markAsReadOnly();
        storage.truncate(10);
    }
}