package io.joshworks.fstore.log;

import io.joshworks.fstore.core.io.MMapStorage;
import io.joshworks.fstore.core.io.Storage;

import java.io.File;
import java.nio.channels.FileChannel;

public class WithMMapStorage extends LogSegmentTest {

    @Override
    Storage getStorage(File file, long size) {
        return new MMapStorage(file, size, FileChannel.MapMode.READ_WRITE);
    }
}
