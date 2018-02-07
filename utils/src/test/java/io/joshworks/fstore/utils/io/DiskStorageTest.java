package io.joshworks.fstore.utils.io;

import java.io.File;

public class DiskStorageTest extends StorageTest {

    @Override
    protected Storage store(File file) {
        return new DiskStorage(file);
    }
}