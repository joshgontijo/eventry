package io.joshworks.fstore.log;

import io.joshworks.fstore.core.io.MMapStorage;
import io.joshworks.fstore.core.io.Mode;
import io.joshworks.fstore.core.io.Storage;
import org.junit.Ignore;

import java.io.File;

@Ignore
public class WithMMapBaseStorage extends LogSegmentTest {

    @Override
    Storage getStorage(File file, long size) {
        return new MMapStorage(file, size, Mode.READ_WRITE, (int)size);
    }
}
