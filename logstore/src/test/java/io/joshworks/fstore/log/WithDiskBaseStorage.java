package io.joshworks.fstore.log;

import io.joshworks.fstore.core.io.DiskStorage;
import io.joshworks.fstore.core.io.Storage;

import java.io.File;

public class WithDiskBaseStorage extends LogSegmentTest {

    @Override
    Storage getStorage(File file, long size) {
        return new DiskStorage(file, size);
    }
}
