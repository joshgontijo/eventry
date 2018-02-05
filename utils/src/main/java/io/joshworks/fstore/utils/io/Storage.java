package io.joshworks.fstore.utils.io;

import java.io.Closeable;
import java.io.Flushable;
import java.nio.ByteBuffer;

public interface Storage extends Flushable, Closeable {

//    int write(ByteBuffer data);
//    int read(ByteBuffer data);

    int write(long position, ByteBuffer data);
    int read(long position, ByteBuffer data);

    long size();

}
