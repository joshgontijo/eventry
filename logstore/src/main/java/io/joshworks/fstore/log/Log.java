package io.joshworks.fstore.log;

import java.io.Closeable;

public interface Log<T> extends Writer<T>, Closeable {

    Reader<T> reader();

    Reader<T> reader(long position);

    long position();

}
