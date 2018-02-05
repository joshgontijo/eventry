package io.joshworks.fstore.utils.io;

import java.io.RandomAccessFile;

public class DiskStorageTest extends StorageTest {

    @Override
    protected Storage store(RandomAccessFile raf) {
        return new DiskStorage(raf);
    }
}