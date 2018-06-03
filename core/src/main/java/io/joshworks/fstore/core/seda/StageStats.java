package io.joshworks.fstore.core.seda;

public class StageStats {

    public final boolean closed;
    public final int activeCount;
    public final int corePoolSize;
    public final int largestPoolSize;
    public final int poolSize;
    public final int maximumPoolSize;
    public final long completedTaskCount;
    public final long taskCount;
    public final int remainingQueueCapacity;
    public final int queueSize;
    public final double averageExecutionTime;
    public final double averageQueueTime;
    public final long totalExecutionTime;
    private final long rejectedTasks;

    StageStats(SedaThreadPoolExecutor threadPool, boolean closed) {
        this.activeCount = threadPool.getActiveCount();
        this.corePoolSize = threadPool.getCorePoolSize();
        this.largestPoolSize = threadPool.getLargestPoolSize();
        this.poolSize = threadPool.getPoolSize();
        this.maximumPoolSize = threadPool.getMaximumPoolSize();
        this.completedTaskCount = threadPool.getCompletedTaskCount();
        this.taskCount = threadPool.getTaskCount();
        this.remainingQueueCapacity = threadPool.getQueue().remainingCapacity();
        this.queueSize = threadPool.getQueue().size();
        this.averageExecutionTime = threadPool.averageExecutionTime();
        this.averageQueueTime = threadPool.averageTimeInQueue();
        this.totalExecutionTime = threadPool.totalTime();
        this.rejectedTasks = threadPool.rejectedTasks();
        this.closed = closed;
    }

    @Override
    public String toString() {
        return "closed=" + closed +
                ", activeCount=" + activeCount +
                ", corePoolSize=" + corePoolSize +
                ", largestPoolSize=" + largestPoolSize +
                ", poolSize=" + poolSize +
                ", maximumPoolSize=" + maximumPoolSize +
                ", completedTaskCount=" + completedTaskCount +
                ", taskCount=" + taskCount +
                ", remainingQueueCapacity=" + remainingQueueCapacity +
                ", queueSize=" + queueSize +
                ", rejectedTasks=" + rejectedTasks +
                ", averageExecutionTime=" + String.format("%.6f", averageExecutionTime) +
                ", totalExecutionTime=" + totalExecutionTime +
                ", averageQueueTime=" + averageQueueTime +
                '}';
    }
}
