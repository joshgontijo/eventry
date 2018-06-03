package io.joshworks.fstore.core.seda;

public class TimedRunnable implements Runnable {

    public final long queuedTime = System.currentTimeMillis();
    private final Runnable delegate;

    private TimedRunnable(Runnable delegate) {
        this.delegate = delegate;
    }

    public static Runnable wrap(Runnable delegate) {
        return new TimedRunnable(delegate);
    }

    @Override
    public void run() {
        delegate.run();
    }
}
