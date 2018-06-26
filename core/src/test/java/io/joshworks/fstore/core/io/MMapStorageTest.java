package io.joshworks.fstore.core.io;

import io.joshworks.fstore.core.utils.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class MMapStorageTest {

    private File file;

    @Before
    public void setUp() {
        file = Utils.testFile();
    }

    @After
    public void tearDown() {
        Utils.tryDelete(file);
    }

    @Test
    public void write() {

        int bufferSize = 1024;
        int fileSize = bufferSize * 100000;
        try (var storage = new MMapStorage(file, fileSize, Mode.READ_WRITE, bufferSize)) {

            var bb = ByteBuffer.allocate(1);
            bb.put((byte) 1).flip();
            for (int i = 0; i < fileSize; i++) {
                storage.write(bb);
                bb.clear();
            }

            var read = ByteBuffer.allocate(1);
            for (int i = 0; i < fileSize; i++) {
                storage.read(i, read);
                read.flip();
                assertEquals(1, read.get());
                read.clear();
            }
        }
    }

    @Test
    public void write2() {

        int entrySize = 4096;
        int bufferSize = entrySize * 10;
        long fileSize = (long) bufferSize * 100000;
        try (var storage = new MMapStorage(file, fileSize, Mode.READ_WRITE, bufferSize)) {

            var bb = ofSize(entrySize);
            long written = 0;
            do {
                written += storage.write(bb);
                bb.clear();
            } while (written < fileSize);


            var readBuffer = ByteBuffer.allocate(entrySize);
            var read = 0;
            do {
                read += storage.read(read, readBuffer);
                readBuffer.flip();
                assertEquals(read, entrySize);
                assertEquals(1, readBuffer.get());
                readBuffer.clear();
            } while (read < fileSize);
        }
    }

    @Test
    public void name() {
        int entrySize = 4096;
        long fileSize = (long)entrySize * 1000000;
        int bufferSize = (int) (fileSize / 2);
        try (var storage = new MMapStorage(file, fileSize, Mode.READ_WRITE, bufferSize)) {

            var bb = ofSize(entrySize);
            for (int i = 0; i < 1000000; i++) {
                bb.clear();
                storage.write(bb);
                storage.flush();
            }

        }

    }

    @Test
    public void write_uneven() {

        int entrySize = 4096;
        int bufferSize = entrySize * 2;
        int fileSize = bufferSize * 2;
        try (var storage = new MMapStorage(file, fileSize, Mode.READ_WRITE, bufferSize)) {

            var bb = ofSize(entrySize + 2);
            long written = 0;
            do {
                written += storage.write(bb);
                bb.clear();
            } while (written < fileSize);


            var readBuffer = ByteBuffer.allocate(entrySize + 2);
            var read = 0;
            do {
                read += storage.read(read, readBuffer);
                readBuffer.flip();
                assertEquals(1, readBuffer.get());
                readBuffer.clear();
            } while (read < fileSize);
        }


    }


    private ByteBuffer ofSize(int size) {
        ByteBuffer bb = ByteBuffer.allocate(size);
        for (int i = 0; i < size; i++) {
            bb.put((byte) 1);
        }
        bb.flip();
        return bb;
    }


    @Test
    public void read() {
    }

    @Test
    public void position() {
    }

    @Test
    public void position1() {
    }

    @Test
    public void delete() {
    }

    @Test
    public void close() {
    }

    @Test
    public void shrink() {
    }

    @Test
    public void flush() {
    }
}