package io.joshworks.fstore.log;

import io.joshworks.fstore.core.io.Storage;

import java.io.Closeable;
import java.nio.ByteBuffer;

public interface Log<T> extends Writer<T>, Closeable {

    int HEADER_SIZE = Integer.BYTES + Integer.BYTES; //length + crc32

    Scanner<T> scanner();

    Scanner<T> scanner(long position);

    long position();

    T get(long position);

    T get(long position, int length);

    long entries();

    long size();

    static long write(Storage storage, ByteBuffer bytes, long position) {
        ByteBuffer bb = ByteBuffer.allocate(HEADER_SIZE + bytes.remaining());
        bb.putInt(bytes.remaining());
        bb.putInt(Checksum.crc32(bytes));
        bb.put(bytes);

        bb.flip();
        int written = storage.write(position, bb);
        return position + written;
    }
}
