package io.joshworks.fstore.log;

import io.joshworks.fstore.core.io.Mode;
import io.joshworks.fstore.core.io.RafStorage;
import io.joshworks.fstore.core.io.Storage;

import java.io.File;

public class WithDiskBaseStorage extends SegmentTest {

    @Override
    Storage getStorage(File file, long size) {
        return new RafStorage(file, size, Mode.READ_WRITE);
    }
}
