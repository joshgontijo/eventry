package io.joshworks.fstore.core.io;

import java.nio.ByteBuffer;

public abstract class DataReader {

    protected final int maxRecordSize;

    protected DataReader(int maxRecordSize) {
        this.maxRecordSize = maxRecordSize;
    }

    public abstract ByteBuffer readForward(Storage storage, long position);

    public abstract ByteBuffer readBackward(Storage storage, long position);

    public abstract ByteBuffer getBuffer();

}
