package io.joshworks.fstore.log;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;

public interface PollingSubscriber<T> extends Closeable {

    T poll() throws InterruptedException;

    T poll(long limit, TimeUnit timeUnit) throws InterruptedException;

    boolean endOfLog();

    long position();

}
