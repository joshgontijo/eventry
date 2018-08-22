package io.joshworks.fstore.log;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;

public interface PollingSubscriber<T> extends Closeable {

    int NO_SLEEP = -1;

    T peek() throws InterruptedException;

    T poll() throws InterruptedException;

    T poll(long limit, TimeUnit timeUnit) throws InterruptedException;

    T take() throws InterruptedException;

    boolean endOfLog();

    long position();

}
