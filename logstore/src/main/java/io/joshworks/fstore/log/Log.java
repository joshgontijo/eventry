package io.joshworks.fstore.log;

import io.joshworks.fstore.core.io.Storage;

import java.io.Closeable;
import java.nio.ByteBuffer;

public interface Log<T> extends Writer<T>, Closeable {

    int HEADER_SIZE = Integer.BYTES + Integer.BYTES; //length + crc32
    int EOF = -1;
    int EOF_SIZE = HEADER_SIZE;

    Scanner<T> scanner();

    Scanner<T> scanner(long position);

    long position();

    T get(long position);

    T get(long position, int length);

    long entries();

    long size();

    static long write(Storage storage, ByteBuffer bytes, long position) {
        ByteBuffer bb = ByteBuffer.allocate(HEADER_SIZE + bytes.remaining() + EOF_SIZE);
        bb.putInt(bytes.remaining());
        bb.putInt(Checksum.crc32(bytes));
        bb.put(bytes);
        addEOF(bb);

        bb.flip();
        int written = storage.write(position, bb);
        return position + written - EOF_SIZE;
    }

    static void addEOF(ByteBuffer bb) {
        bb.putInt(EOF);
        bb.putInt(0);
    }

}
