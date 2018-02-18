package io.joshworks.fstore.log;

import java.io.Closeable;

public interface Log<T> extends Writer<T>, Closeable {

    Scanner<T> scanner();

    Scanner<T> scanner(long position);

    long position();

    T get(long position);

    T get(long position, int length);

    long entries();

    long size();

}
