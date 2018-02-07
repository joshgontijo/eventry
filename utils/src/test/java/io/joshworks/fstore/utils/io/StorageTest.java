package io.joshworks.fstore.utils.io;

import io.joshworks.fstore.utils.IOUtils;
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

    @Test
    public void write_size() {
        ByteBuffer bb = ByteBuffer.wrap(TEST_DATA.getBytes(StandardCharsets.UTF_8));
        int written = storage.write(0, bb);
        assertEquals(TEST_DATA.length(), written);
    }

    @Test
    public void write_read() {
        ByteBuffer bb = ByteBuffer.wrap(TEST_DATA.getBytes(StandardCharsets.UTF_8));
        int write = storage.write(0, bb);

        ByteBuffer result = ByteBuffer.allocate(bb.capacity());
        int read = storage.read(0, result);

        assertEquals(write, read);
        assertTrue(Arrays.equals(bb.array(), result.array()));
    }

    @Test
    public void write_large() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            sb.append(UUID.randomUUID().toString());
        }

        String longString = sb.toString();

        ByteBuffer bb = ByteBuffer.wrap(longString.getBytes(StandardCharsets.UTF_8));
        int write = storage.write(0, bb);

        ByteBuffer result = ByteBuffer.allocate(bb.capacity());
        int read = storage.read(0, result);

        assertEquals(write, read);
        assertTrue(Arrays.equals(bb.array(), result.array()));
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