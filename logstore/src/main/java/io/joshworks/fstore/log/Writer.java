package io.joshworks.fstore.log;

public interface Writer<T> {

    long write(T data);

}
