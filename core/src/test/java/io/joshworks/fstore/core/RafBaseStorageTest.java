package io.joshworks.fstore.core;

import io.joshworks.fstore.core.io.Mode;
import io.joshworks.fstore.core.io.RafStorage;
import io.joshworks.fstore.core.io.Storage;

import java.io.File;

public class RafBaseStorageTest extends DiskStorageTest {

    @Override
    protected Storage store(File file, long size) {
        return new RafStorage(file, size, Mode.READ_WRITE);
    }
}