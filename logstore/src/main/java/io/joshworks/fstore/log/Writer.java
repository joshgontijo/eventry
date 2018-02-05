package io.joshworks.fstore.log;

import java.io.Flushable;

public interface Writer<T> extends Flushable {

    long append(T data);

}
