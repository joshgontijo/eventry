package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.io.MMapStorage;
import io.joshworks.fstore.core.io.Mode;
import io.joshworks.fstore.core.io.RafStorage;
import io.joshworks.fstore.core.io.Storage;

import java.io.File;
import java.util.Objects;

public class StorageProvider {

    private final boolean mmap;
    private final int mmapBufferSize;

    private StorageProvider(boolean mmap, int mmapBufferSize) {
        this.mmap = mmap;
        this.mmapBufferSize = mmapBufferSize;
    }

    static StorageProvider mmap(int bufferSize) {
        return new StorageProvider(true, bufferSize);
    }

    static StorageProvider raf() {
        return new StorageProvider(false, -1);
    }

    public Storage create(File file, long length) {
        Objects.requireNonNull(file, "File must be provided");
        return mmap ? new MMapStorage(file, length, Mode.READ_WRITE, mmapBufferSize) : new RafStorage(file, length, Mode.READ_WRITE);
    }

    public Storage open(File file) {
        Objects.requireNonNull(file, "File must be provided");
        return mmap ? new MMapStorage(file, file.length(), Mode.READ_WRITE, mmapBufferSize) : new RafStorage(file, file.length(), Mode.READ_WRITE);
    }

}
