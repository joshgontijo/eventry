package io.joshworks.fstore.log;

import java.util.Iterator;

public interface LogIterator<T> extends Iterator<T> {
    long position();

}
