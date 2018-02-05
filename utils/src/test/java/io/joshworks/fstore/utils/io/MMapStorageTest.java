package io.joshworks.fstore.utils.io;

import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public class MMapStorageTest extends StorageTest {

    @Override
    protected Storage store(RandomAccessFile raf) {
        return new MMapStorage(raf, FileChannel.MapMode.READ_WRITE);
    }
}