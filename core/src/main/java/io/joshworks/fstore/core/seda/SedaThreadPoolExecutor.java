package io.joshworks.fstore.core.seda;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SedaThreadPoolExecutor extends ThreadPoolExecutor {

    private final RejectedExecutionHandlerWrapper rejectionHandler;
    private final AtomicLong totalTime = new AtomicLong();
    private final AtomicLong totalTimeInQueue = new AtomicLong();
    private final AtomicLong totalExecutions = new AtomicLong();
    private final ThreadLocal<TimeWatch> executionTimer = new ThreadLocal<>();
    private final TimeWatch timer = TimeWatch.start();

    private SedaThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, SedaThreadFactory threadFactory, LimitBlockingQueue<Runnable> queue, RejectedExecutionHandlerWrapper rejectionHandler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, queue, threadFactory, rejectionHandler);
        this.rejectionHandler = rejectionHandler;
    }

    public static SedaThreadPoolExecutor create(String name, int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, int queueSize, int queueHighBound, RejectedExecutionHandler handler, boolean blockWhenFull) {
        LimitBlockingQueue<Runnable> queue = new LimitBlockingQueue<>(queueHighBound, queueSize);
        SedaThreadFactory threadFactory = new SedaThreadFactory(name);
        RejectedExecutionHandlerWrapper rejectionHandler = new RejectedExecutionHandlerWrapper(handler, blockWhenFull);
        return new SedaThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, unit, threadFactory, queue, rejectionHandler);
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {

        TimedRunnable tr = (TimedRunnable)r;
        totalTimeInQueue.addAndGet(System.currentTimeMillis() - tr.queuedTime);


        super.beforeExecute(t, r);
        executionTimer.set(TimeWatch.start());
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        long time = executionTimer.get().time();
        totalExecutions.incrementAndGet();
        totalTime.addAndGet(time);
    }

    long totalTime() {
        return totalTime.get();
    }

    double averageExecutionTime() {
        long totalTasks = totalExecutions.get();
        return (totalTasks == 0) ? 0 : totalTime.get() / (double) totalTasks;
    }

    double averageTimeInQueue() {
        long totalTasks = totalExecutions.get();
        return (totalTasks == 0) ? 0 : totalTimeInQueue.get() / (double) totalTasks;
    }

    long rejectedTasks() {
        return rejectionHandler.rejectedTasksCount();
    }

    private static class RejectedExecutionHandlerWrapper implements RejectedExecutionHandler {

        private final RejectedExecutionHandler delegate;
        private final boolean blockWhenFull;
        private final AtomicLong rejectedCount = new AtomicLong();

        public RejectedExecutionHandlerWrapper(RejectedExecutionHandler handler, boolean blockWhenFull) {
            this.delegate = handler;
            this.blockWhenFull = blockWhenFull;
        }

        @Override
        public void rejectedExecution(Runnable task, ThreadPoolExecutor executor) {
            try {
                BlockingQueue<Runnable> queue = executor.getQueue();
                if(blockWhenFull) {
                    queue.put(task);
                } else {
                    queue.add(task);
                }
            } catch (IllegalStateException | InterruptedException e) {
                rejectedCount.incrementAndGet();
                delegate.rejectedExecution(task, executor);
                throw new EnqueueException();
            }
        }

        private long rejectedTasksCount() {
            return rejectedCount.get();
        }
    }

    private static class LimitBlockingQueue<T> extends LinkedBlockingQueue<T> {

        private final int highBound;

        private LimitBlockingQueue(int highBound, int capacity) {
            super(capacity);
            this.highBound = highBound;
        }

        @Override
        public boolean offer(T t) {
            int size = size();
            boolean offer = super.offer(t);

            return size <= highBound && offer;
        }
    }

    private static class SedaThreadFactory implements ThreadFactory {
        private final AtomicInteger poolNumber = new AtomicInteger(1);
        private final String prefix;

        private SedaThreadFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setName(prefix + "-" + poolNumber.getAndIncrement());
            return thread;
        }
    }

}
