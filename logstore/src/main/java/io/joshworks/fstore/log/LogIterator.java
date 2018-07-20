package io.joshworks.fstore.log;

import java.io.Closeable;
import java.util.Iterator;

public interface LogIterator<T> extends Iterator<T>, Closeable {
    long position();

}
