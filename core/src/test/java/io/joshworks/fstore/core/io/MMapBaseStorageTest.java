package io.joshworks.fstore.core.io;

import io.joshworks.fstore.core.utils.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@Ignore
public class MMapBaseStorageTest extends DiskStorageTest {

    private File mmapFile;
    private static final int BUFFER_SIZE = 5408192;

    @Override
    protected Storage store(File file, long size) {
        return new MMapStorage(file, size, Mode.READ_WRITE, BUFFER_SIZE);
    }


    @Before
    public void setUpMMapFile() {
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
    public void data_is_never_split() {
        int entrySize = 4096;
        int bufferSize = entrySize * 2;
        long fileSize = (long) bufferSize * 2;
        try (var storage = new MMapStorage(mmapFile, fileSize, Mode.READ_WRITE, bufferSize)) {

            var bb = ofSize(entrySize);
            storage.write(bb);

            var bb2 = ofSize(entrySize + 1);
            storage.write(bb2);

            assertEquals(2, storage.buffers.size());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void read_is_limited_by_remaining_buffer_size() {
        int entrySize = 4096;
        int bufferSize = entrySize * 2;
        long fileSize = (long) bufferSize * 2;
        try (var storage = new MMapStorage(mmapFile, fileSize, Mode.READ_WRITE, bufferSize)) {

            var bb = ofSize(bufferSize + 1);
            storage.write(bb);

        }
    }

    @Test
    public void read_returns_correct_data_with_multiple_buffers() {
        int entrySize = 4096;
        int bufferSize = entrySize * 2;
        long fileSize = (long) bufferSize * 2;
        try (var storage = new MMapStorage(mmapFile, fileSize, Mode.READ_WRITE, bufferSize)) {

            for (int i = 0; i < 10; i++) {
                var bb = ofSize(entrySize);
                storage.write(bb);
            }

            var expected = ofSize(entrySize);
            for (int i = 0; i < 10; i++) {
                var read = ByteBuffer.allocate(entrySize);
                storage.read(i * entrySize, read);

                assertArrayEquals(expected.array(), read.array());
            }

        }
    }

    @Test
    public void readonly_file_returns_correct_data() {
        int entrySize = 1024;
        int bufferSize = entrySize * 2;
        long fileSize = (long) bufferSize * 2;
        try (var storage = new MMapStorage(mmapFile, fileSize, Mode.READ_WRITE, bufferSize)) {

            var bb = ofSize(entrySize);
            storage.write(bb);

            storage.markAsReadOnly();

            var read = ByteBuffer.allocate(entrySize);
            storage.read(0, read);

            assertArrayEquals(bb.array(), read.flip().array());
        }
    }

    @Test
    public void entry_cannot_be_greater_than_buffer_size() {
        int entrySize = 4096;
        int bufferSize = entrySize * 2;
        long fileSize = (long) bufferSize * 2;
        try (var storage = new MMapStorage(mmapFile, fileSize, Mode.READ_WRITE, bufferSize)) {

            var bb = ofSize(entrySize);
            storage.write(bb);

            var read = ByteBuffer.allocate(bufferSize + 1);
            storage.read(0, read);

            assertEquals(entrySize, read.flip().remaining());
        }
    }

    @Test
    public void shrink_resize_the_file_rounding_to_the_buffer_size() {
        ByteBuffer bb = ByteBuffer.wrap(TEST_DATA.getBytes(StandardCharsets.UTF_8));
        storage.write(bb);

        long pos = storage.position();
        storage.truncate(pos);

        assertEquals(BUFFER_SIZE, storage.length());
    }

    private ByteBuffer ofSize(int size) {
        ByteBuffer bb = ByteBuffer.allocate(size);
        for (int i = 0; i < size; i++) {
            bb.put((byte) 1);
        }
        bb.flip();
        return bb;
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