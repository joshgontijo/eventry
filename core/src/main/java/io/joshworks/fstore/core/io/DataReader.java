package io.joshworks.fstore.core.io;

import java.nio.ByteBuffer;

public abstract class DataReader {

    protected final Storage storage;

    public DataReader(Storage storage) {
        this.storage = storage;
    }

    public abstract ByteBuffer read(long position);

    public abstract ByteBuffer read(long position, int size);


}
