package io.joshworks.fstore.log.segment;

import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.Writer;

import java.io.Closeable;
import java.util.stream.Stream;

public interface Log<T> extends Writer<T>, Closeable {

    int ENTRY_HEADER_SIZE = Integer.BYTES * 2; //length + crc32
    byte[] EOF = new byte[]{0xFFFFFFFF, 0x00000000}; //eof header, -1 length, 0 crc
    long START = Header.SIZE;

    String name();

    LogIterator<T> iterator();

    Stream<T> stream();

    LogIterator<T> iterator(long position);

    long position();

    T get(long position);

    long size();

    SegmentState rebuildState(long lastKnownPosition);

    void delete();

    void roll(int level);

    boolean readOnly();

    long entries();

    int level();

    long created();

}
