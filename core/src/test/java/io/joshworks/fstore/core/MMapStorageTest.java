package io.joshworks.fstore.core;

import io.joshworks.fstore.core.io.MMapStorage;
import io.joshworks.fstore.core.io.Storage;

import java.io.File;
import java.nio.channels.FileChannel;

public class MMapStorageTest extends StorageTest {

    @Override
    protected Storage store(File file) {
        return new MMapStorage(file, FileChannel.MapMode.READ_WRITE);
    }
}