package io.joshworks.fstore.core;

import io.joshworks.fstore.core.io.MMapStorage;
import io.joshworks.fstore.core.io.Storage;

import java.io.File;
import java.nio.channels.FileChannel;

public class MMapBaseStorageTest extends BaseStorageTest {

    @Override
    protected Storage store(File file) {
        return new MMapStorage(file, FileChannel.MapMode.READ_WRITE);
    }

    @Override
    protected Storage store(File file, long size) {
        return new MMapStorage(file, size, FileChannel.MapMode.READ_WRITE);
    }
}