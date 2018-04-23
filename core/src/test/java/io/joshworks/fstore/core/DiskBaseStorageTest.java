package io.joshworks.fstore.core;

import io.joshworks.fstore.core.io.DiskStorage;
import io.joshworks.fstore.core.io.Storage;

import java.io.File;

public class DiskBaseStorageTest extends BaseStorageTest {

    @Override
    protected Storage store(File file) {
        return new DiskStorage(file);
    }

    @Override
    protected Storage store(File file, long size) {
        return new DiskStorage(file, size);
    }
}