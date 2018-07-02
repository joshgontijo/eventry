package io.joshworks.fstore.core.io;

import java.io.File;

public class RafBaseStorageTest extends DiskStorageTest {

    @Override
    protected Storage store(File file, long size) {
        return new RafStorage(file, size, Mode.READ_WRITE);
    }
}