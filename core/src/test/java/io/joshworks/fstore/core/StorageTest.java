package io.joshworks.fstore.core;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.Storage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class StorageTest {

    private static final String TEST_DATA = "TEST-DATA";
    private Storage storage;
    private Path testFile;

    protected abstract Storage store(File file);

    protected abstract Storage store(File file, long size);

    @Before
    public void setUp() throws Exception {
        testFile = new File("storage.db").toPath();
        storage = store(testFile.toFile());
    }

    @After
    public void cleanup() {
        IOUtils.closeQuietly(storage);
        tryRemoveFile();
    }

    @Test(expected = IllegalArgumentException.class)
    public void when_witting_empty_data_throw_exception() {
        storage.write(ByteBuffer.allocate(0));
    }

    @Test
    public void when_data_is_written_return_th_written_length() {
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
        int entrySize = 36; //uuid byte size

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
            assertEquals(entrySize, read);

            bb.flip();
            String found = new String(bb.array(), StandardCharsets.UTF_8);
            assertTrue("Not found: [" + found + "] at offset " + offset + ", item number: " + itemsRead, inserted.contains(found));
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
        Path tobeDeleted = Files.createTempFile("tobeDeleted", null);
        Storage store = store(tobeDeleted.toFile());
        store.delete();

        assertFalse(Files.exists(tobeDeleted));
    }

    @Test
    public void when_data_is_written_the_size_must_increase() {
        int size = Integer.BYTES;
        ByteBuffer bb = ByteBuffer.allocate(size);
        bb.putInt(1).flip();

        storage.write(bb);

        assertEquals(size, storage.size());
    }

    @Test
    public void when_position_is_set_the_size_must_be_the_same() throws IOException {
        long thePosition = 4;
        Path temp = Files.createTempFile("tobeDeleted", null);
        Storage store = store(temp.toFile(), thePosition);
        store.position(thePosition);

        assertEquals(thePosition, store.size());
    }

    //terrible work around for waiting the mapped buffer to release file lock
    private void tryRemoveFile() {
        int maxTries = 50;
        int counter = 0;
        while (counter++ < maxTries) {
            try {
                Files.delete(testFile);
                break;
            }catch (Exception e) {
                System.err.println(":: LOCK NOT RELEASED YET ::");
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }
}