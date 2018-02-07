package io.joshworks.fstore.utils.io;

import java.io.File;
import java.nio.channels.FileChannel;

public class MMapStorageTest extends StorageTest {

    @Override
    protected Storage store(File file) {
        return new MMapStorage(file, FileChannel.MapMode.READ_WRITE);
    }
}