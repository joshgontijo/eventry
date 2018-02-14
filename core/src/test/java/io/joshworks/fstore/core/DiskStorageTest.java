package io.joshworks.fstore.core;

import io.joshworks.fstore.core.io.DiskStorage;
import io.joshworks.fstore.core.io.Storage;

import java.io.File;

public class DiskStorageTest extends StorageTest {

    @Override
    protected Storage store(File file) {
        return new DiskStorage(file);
    }
}