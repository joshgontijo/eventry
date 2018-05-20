package io.joshworks.fstore.core;

import io.joshworks.fstore.core.io.MMapStorage;
import io.joshworks.fstore.core.io.Mode;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.utils.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MMapBaseStorageTest extends DiskStorageTest {

    private File mmapFile;

    @Override
    protected Storage store(File file, long size) {
        return new MMapStorage(file, size, Mode.READ_WRITE, (int) size);
    }

    @Before
    public void mmapStorageSpecificSetup() {
        mmapFile = Utils.testFile();
    }

    @After
    public void tearDown() {
        Utils.tryDelete(mmapFile);
    }

    @Test
    public void multiple_buffers() throws IOException {
        long fileSize = 1024;
        int bufferSize = 512;
        try(Storage storage = create(fileSize, bufferSize, Mode.READ_WRITE)) {

            int dataSize = bufferSize / 2;
            //first
            storage.write(dummyData(dataSize));
            assertEquals(dataSize, storage.position());

            storage.write(dummyData(dataSize));
            assertEquals(dataSize * 2, storage.position());

            //second
            storage.write(dummyData(dataSize));
            assertEquals(dataSize * 3, storage.position());

            storage.write(dummyData(dataSize));
            assertEquals(dataSize * 4, storage.position());

            assertEquals(fileSize, storage.position());
        }
    }

    @Test
    public void when_no_buffer_is_available_a_new_one_is_created() throws IOException {
        long fileSize = 16;
        int bufferSize = 16;
        try(Storage storage = create(fileSize, bufferSize, Mode.READ_WRITE)) {

            int dataSize = bufferSize * 2;
            //first
            ByteBuffer dummyData = dummyData(dataSize);
            storage.write(dummyData);
            assertEquals(dataSize, storage.position());
            assertEquals(dataSize, storage.length());

            ByteBuffer found = ByteBuffer.allocate(dataSize);
            storage.read(0, found);

            assertTrue(Arrays.equals(dummyData.array(), found.array()));
        }
    }


    private static ByteBuffer dummyData(int size) {
        byte[] data = new byte[size];
        Arrays.fill(data, (byte) 1);
        return ByteBuffer.wrap(data);
    }

    private Storage create(long fileSize, int bufferSize, Mode mode) {
        return new MMapStorage(mmapFile, fileSize, mode, bufferSize);
    }
}