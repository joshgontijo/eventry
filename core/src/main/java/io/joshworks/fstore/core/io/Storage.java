package io.joshworks.fstore.core.io;

import java.io.Closeable;
import java.io.Flushable;
import java.nio.ByteBuffer;

public interface Storage extends Flushable, Closeable {

    int write(ByteBuffer data);

    int read(long position, ByteBuffer data);

    long length();

    void position(long position);

    long position();

    void delete();

    String name();

    void truncate(long pos);

}
