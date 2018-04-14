package io.joshworks.fstore.core;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.Storage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class StorageTest {

    private static final String TEST_DATA = "TEST-DATA";
    private Storage storage;
    private Path testFile;

    protected abstract Storage store(File raf) throws Exception;

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
    public void when_() {
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
    public void ensureCapacity() {

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