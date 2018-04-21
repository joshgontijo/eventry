package io.joshworks.fstore.log;

import java.io.Closeable;
import java.util.stream.Stream;

public interface Log<T> extends Writer<T>, Closeable {

    int ENTRY_HEADER_SIZE = Integer.BYTES * 2; //length + crc32

    String name();

    Scanner<T> scanner();

    Stream<T> stream();

    Scanner<T> scanner(long position);

    long position();

    T get(long position);

    long size();

    long checkIntegrity(long lastKnownPosition);

    void delete();

    void complete();

}
