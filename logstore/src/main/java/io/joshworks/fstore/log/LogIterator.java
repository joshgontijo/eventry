package io.joshworks.fstore.log;

public interface LogIterator<T> extends CloseableIterator<T> {
    long position();

}
